(ns kafka-study.day3.load
  (:require [kafka-study.day3.sensors :as s]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.time Duration]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]
           [org.apache.kafka.clients.admin AdminClient OffsetSpec ListOffsetsResult$ListOffsetsResultInfo]
           [org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata ConsumerRebalanceListener
            ConsumerRecord CommitFailedException]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringDeserializer]))

(def default-topic "sensors-load")
(def default-group "detector")
(def n-partitions 6)
(def n-sensors 1000)

;; 処理した「パーティション-offset」ごとの回数。2回以上あれば重複して処理された
(defonce seen (ConcurrentHashMap.))

(def ^:private sum-fn
  (reify java.util.function.BiFunction
    (apply [_ a b] (+ (long a) (long b)))))

(defn reset-seen! []
  (.clear ^ConcurrentHashMap seen))

(defn dup-stats
  "処理した延べ件数・種類数・重複して処理された件数を返す。"
  []
  (let [vs    (map long (.values ^ConcurrentHashMap seen))
        total (reduce + 0 vs)]
    {:processed  total
     :distinct   (count vs)
     :duplicates (- total (count vs))}))

;; =========================================================
;; センサー側：決めたペースで送り続ける
;; =========================================================

(defn- send-n! [^KafkaProducer p topic ^AtomicLong counter n]
  (dotimes [_ n]
    (let [k   (.getAndIncrement counter)
          sid (s/sensor-id (mod k n-sensors))]
      (.send p (ProducerRecord. topic sid (s/make-reading sid (quot k n-sensors)))))))

(defn run-load!
  "phases = [[秒数 1秒あたりの件数] ...] の順に、そのペースで送る。
   別スレッドで動き、全部送り終えたら送った件数を返す。"
  [phases & {:keys [topic] :or {topic default-topic}}]
  (future
    (with-open [p (s/make-producer)]
      (let [counter (AtomicLong.)]
        (doseq [[secs rate] phases]
          (let [start (System/nanoTime)
                total (* secs rate)]
            (loop [sent 0]
              (when (< sent total)
                (let [elapsed (/ (- (System/nanoTime) start) 1e9)
                      target  (min total (long (* rate elapsed)))]
                  (send-n! p topic counter (- target sent))
                  (Thread/sleep 5)
                  (recur target))))))
        (.flush p)
        (.get counter)))))

;; =========================================================
;; 受け手側：異常値を検知する consumer
;; =========================================================

(defn- make-consumer [group props]
  (KafkaConsumer. ^java.util.Map (merge {"bootstrap.servers"  s/bootstrap
                                         "group.id"           group
                                         "auto.offset.reset"  "earliest"
                                         "enable.auto.commit" "false"}
                                        props)
                  (StringDeserializer.)
                  (StringDeserializer.)))

(defn- partitions-str [parts]
  (vec (sort (map #(.partition ^TopicPartition %) parts))))

(defn- rebalance-listener
  "担当パーティションが外されたとき・割り当てられたときに表示する。"
  [id]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ parts]
      (println (str "[" id "] revoked  " (partitions-str parts))))
    (onPartitionsAssigned [_ parts]
      (println (str "[" id "] assigned " (partitions-str parts))))))

(defn- worker-loop [id topic group props proc-ms stop? crash? ^AtomicLong processed]
  (try
    (with-open [c (make-consumer group props)]
      (.subscribe c [topic] (rebalance-listener id))
      (loop []
        (when-not @stop?
          (let [recs (.poll c (Duration/ofMillis 200))
                n    (.count recs)]
            (when (pos? n)
              ;; 異常値の判定と保存の代わりに、1件あたり proc-ms の時間をかける
              (Thread/sleep (long (* n proc-ms)))
              (doseq [^ConsumerRecord r recs]
                (.merge ^ConcurrentHashMap seen (str (.partition r) "-" (.offset r)) 1 sum-fn))
              (.addAndGet processed n))
            (cond
              @crash?
              (println (str "[" id "] crash!（" n " 件を処理したが、commit せずに終了）"))

              (pos? n)
              (do (try (.commitSync c)
                       (catch CommitFailedException _
                         (println (str "[" id "] commit 失敗（グループから外されていた）"))))
                  (recur))

              :else
              (recur))))))
    (catch Exception e
      (println (str "[" id "] error: " e)))))

(defonce workers (atom {}))   ; id → {:stop? :fut :processed}

(defn start-workers!
  "consumer を n 台になるまで起動する（同じグループ）。proc-ms は1件あたりの処理時間。
   props で consumer の設定を追加できる。"
  [n proc-ms & {:keys [topic group props]
                :or   {topic default-topic group default-group props {}}}]
  (doseq [i (range 1 (inc n))
          :let [id (str "w" i)]
          :when (not (contains? @workers id))]
    (let [stop?     (atom false)
          crash?    (atom false)
          processed (AtomicLong.)
          fut       (future (worker-loop id topic group props proc-ms stop? crash? processed))]
      (swap! workers assoc id {:stop? stop? :crash? crash? :fut fut :processed processed}))))

(defn crash-worker!
  "その consumer を、処理中の分を commit せずに終了させる（落ちた状態の代わり）。"
  [id]
  (when-let [{:keys [crash? fut]} (get @workers id)]
    (reset! crash? true)
    @fut
    (swap! workers dissoc id)
    id))

(defn stop-worker! [id]
  (when-let [{:keys [stop? fut]} (get @workers id)]
    (reset! stop? true)
    @fut
    (swap! workers dissoc id)
    id))

(defn stop-all! []
  (doseq [id (keys @workers)] (stop-worker! id)))

(defn processed-counts []
  (into (sorted-map) (for [[id {:keys [processed]}] @workers] [id (.get ^AtomicLong processed)])))

;; =========================================================
;; lag の計測
;; =========================================================

(defn- admin []
  (AdminClient/create ^java.util.Map {"bootstrap.servers" s/bootstrap}))

(defn- lag-now
  "パーティションごとの lag（末尾の offset − commit 済みの offset）を返す。"
  [^AdminClient a topic group]
  (let [tps       (mapv #(TopicPartition. topic %) (range n-partitions))
        committed (-> (.listConsumerGroupOffsets a group) (.partitionsToOffsetAndMetadata) (.get))
        ends      (-> (.listOffsets a (into {} (for [tp tps] [tp (OffsetSpec/latest)]))) (.all) (.get))]
    (into (sorted-map)
          (for [^TopicPartition tp tps]
            (let [end (.offset ^ListOffsetsResult$ListOffsetsResultInfo (get ends tp))
                  c   (if-let [^OffsetAndMetadata om (get committed tp)] (.offset om) 0)]
              [(.partition tp) (- end c)])))))

(defn start-monitor!
  "1秒ごとに lag を記録する。{:log 記録の atom, :stop! 止める関数} を返す。"
  [& {:keys [topic group] :or {topic default-topic group default-group}}]
  (let [log   (atom [])
        stop? (atom false)
        fut   (future
                (with-open [a (admin)]
                  (let [start (System/currentTimeMillis)]
                    (loop [i 0]
                      (when-not @stop?
                        (let [by-p  (lag-now a topic group)
                              t     (quot (- (System/currentTimeMillis) start) 1000)
                              total (reduce + (vals by-p))]
                          (swap! log conj {:t t :total total :by-partition by-p})
                          (when (zero? (mod i 10))
                            (println (str "t=" t "s lag=" total " " by-p)))
                          (Thread/sleep 1000)
                          (recur (inc i))))))))]
    {:log log :stop! (fn [] (reset! stop? true) @fut)}))

(defn summarize
  "記録から、lag の最大値とその時刻、lag が threshold 未満に戻った時刻を求める。"
  ([log] (summarize log 1000))
  ([log threshold]
   (let [peak      (apply max-key :total log)
         recovered (->> log
                        (drop-while #(<= (:t %) (:t peak)))
                        (filter #(< (:total %) threshold))
                        first)]
     {:max-lag        (:total peak)
      :max-lag-at-s   (:t peak)
      :recovered-at-s (:t recovered)
      :recovery-s     (when recovered (- (:t recovered) (:t peak)))})))

(defn save-csv!
  "記録を out/day3/<name>.csv に保存する（あとでグラフにするため）。"
  [log name]
  (let [f (io/file "out/day3" (str name ".csv"))]
    (io/make-parents f)
    (spit f (str/join "\n"
                      (cons (str "t,total," (str/join "," (map #(str "p" %) (range n-partitions))))
                            (for [{:keys [t total by-partition]} log]
                              (str/join "," (concat [t total] (vals by-partition)))))))
    (str f)))

;; =========================================================
;; Step 3：溜まった lag を n 台で処理しきるまでの時間
;; =========================================================

(defn drain-test!
  "consumer を n 台起動し、group の lag が 0 になるまでの秒数を測る。
   終わったら consumer を止める。新しい group 名を使うと、topic の先頭から処理する。"
  [n proc-ms & {:keys [topic group props] :or {props {}}}]
  (reset-seen!)
  (with-open [a (admin)]
    (let [initial (lag-now a topic group)
          start   (System/nanoTime)]
      (start-workers! n proc-ms :topic topic :group group :props props)
      (Thread/sleep 1000)
      (loop []
        (if (pos? (reduce + (vals (lag-now a topic group))))
          (do (Thread/sleep 500) (recur))
          (let [secs   (/ (Math/round (/ (- (System/nanoTime) start) 1e8)) 10.0)
                counts (processed-counts)]
            (stop-all!)
            {:consumers             n
             :seconds               secs
             :records-per-partition initial
             :processed-per-worker  counts
             :dup                   (dup-stats)}))))))
