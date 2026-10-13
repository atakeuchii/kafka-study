(ns kafka-study.day2.acks
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str])
  (:import [java.time Duration LocalTime]
           [java.util BitSet]
           [java.util.concurrent.atomic AtomicLong]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

;; 3台のどれが止まっていても繋がるように、全部並べる
(def bootstrap "localhost:9092,localhost:9094,localhost:9096")

(defn- make-producer [acks]
  (KafkaProducer. ^java.util.Map
   {"bootstrap.servers"  bootstrap
    "acks"               acks
    ;; 冪等 producer は acks=all が前提なので、0 と 1 では無効にする
    "enable.idempotence" (if (= acks "all") "true" "false")}
                  (StringSerializer.)
                  (StringSerializer.)))

(defn- schedule-actions!
  "actions = [[ミリ秒 [docker の引数 ...]] ...] を、
   送信開始からの時刻に合わせて順に実行する。"
  [actions]
  (future
    (let [start (System/currentTimeMillis)]
      (doseq [[at-ms args] (sort-by first actions)]
        (let [wait (- at-ms (- (System/currentTimeMillis) start))]
          (when (pos? wait)
            (Thread/sleep (long wait))))
        (let [{:keys [exit err]} (apply sh "docker" args)]
          (println (str "[" (LocalTime/now) "] docker " (str/join " " args)
                        " → exit " exit
                        (when (seq err) (str " / " (str/trim err))))))))))

;; (defn- schedule-kill!
;;   "delay-ms 後に、コンテナを強制終了する。"
;;   [container delay-ms]
;;   (future
;;     (Thread/sleep (long delay-ms))
;;     (println (str "[" (LocalTime/now) "] docker kill " container))
;;     (sh "docker" "kill" container)))

(defn send-with-kill!
  "acks の設定で、topic に 0〜n-1 の通し番号を送る（1秒に約 rate 件）。
   送信開始から kill-at-ms 後に kafka-1 を強制終了する。
   ブローカーから成功が返ってきた番号を記録して返す。"
  [{:keys [acks topic n rate kill-at-ms actions]
    :or   {n 200000 rate 20000 kill-at-ms 3000}}]
  (let [acked  (BitSet.)
        failed (AtomicLong.)
        per-ms (max 1 (quot rate 1000))
        start  (System/nanoTime)]
    (with-open [p (make-producer acks)]
      (let [killer (schedule-actions! (or actions [[kill-at-ms ["kill" "kafka-1"]]]))]
        (dotimes [i n]
          (.send p (ProducerRecord. topic (str i))
                 (reify Callback
                   (onCompletion [_ _ ex]
                     (if ex
                       (.incrementAndGet failed)
                       (locking acked (.set acked (int i)))))))
          ;; per-ms 件送るごとに 1ms 休んで、送るペースを抑える
          (when (zero? (mod (inc i) per-ms))
            (Thread/sleep 1)))
        (.flush p)
        @killer))
    {:acks      acks
     :sent      n
     :acked     (locking acked (.cardinality acked))
     :failed    (.get failed)
     :ms        (Math/round (/ (- (System/nanoTime) start) 1e6))
     :acked-set acked}))

(defn read-all
  "topic のパーティション0を先頭から末尾まで読み、届いている通し番号を記録する。"
  [topic]
  (let [tp      (TopicPartition. topic 0)
        present (BitSet.)]
    (with-open [c (KafkaConsumer. ^java.util.Map {"bootstrap.servers"  bootstrap
                                                  "enable.auto.commit" "false"}
                                  (StringDeserializer.)
                                  (StringDeserializer.))]
      (.assign c [tp])
      (.seekToBeginning c [tp])
      (let [end (get (.endOffsets c [tp]) tp)]
        (loop [records 0]
          (if (< (.position c tp) end)
            (let [recs (.poll c (Duration/ofMillis 500))]
              (doseq [^ConsumerRecord r recs]
                (.set present (Integer/parseInt (.value r))))
              (recur (+ records (.count recs))))
            {:records records :present present}))))))

(defn report
  "送信結果と、実際に読めた内容を照合する。"
  [{:keys [^BitSet acked-set sent] :as result} topic]
  (let [{:keys [records ^BitSet present]} (read-all topic)
        lost (doto ^BitSet (.clone acked-set) (.andNot present))]
    (-> (dissoc result :acked-set)
        (assoc :stored-records  records
               :stored-distinct (.cardinality present)
               :duplicates      (- records (.cardinality present))
               :acked-but-lost  (.cardinality ^BitSet lost)
               :lost-total      (- sent (.cardinality present))))))
