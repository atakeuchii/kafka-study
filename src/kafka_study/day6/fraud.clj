(ns kafka-study.day6.fraud
  "Day 6：不正検知（状態を持つ consumer）
   ルール：同じカードが 5 分以内に 3 か国以上で使われたら不正。"
  (:require [clojure.string :as str]
            [clojure.edn :as edn])
  (:import (java.time Duration Instant ZoneId)
           (java.time.format DateTimeFormatter)
           (java.util ArrayList Collections Random)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRebalanceListener)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)))

;; ============================================================
;; 設定
;; ============================================================

(def bootstrap "localhost:9092,localhost:9094,localhost:9096")
(def replication 3)
(def window-ms (* 5 60 1000))                                  ; 5 分
(def t0 (.toEpochMilli (Instant/parse "2026-10-17T01:00:00Z"))) ; 日本時間 10:00:00（シミュレーション上の開始時刻）
(def countries ["JP" "US" "FR" "KR" "GB" "DE" "CN" "BR"])

;; changelog（状態の控え）用トピックの設定。compact = キーごとに最新だけ残す
(def changelog-configs {"cleanup.policy"            "compact"
                        "segment.ms"                "10000"
                        "min.cleanable.dirty.ratio" "0.01"})

(def ^DateTimeFormatter hms-fmt (DateTimeFormatter/ofPattern "HH:mm:ss"))
(defn hms
  "ミリ秒 → 日本時間の HH:mm:ss"
  [ms]
  (.format (.atZone (Instant/ofEpochMilli ms) (ZoneId/of "Asia/Tokyo")) hms-fmt))

;; ============================================================
;; データ生成（シミュレーション上の時刻を使うので、実際に 5 分待たなくてよい）
;; ============================================================

(defn- card-id [i] (format "card-%04d" i))

(defn- pick-countries
  "重複なしで n か国を選ぶ（seed 付きの乱数で、毎回同じ結果になる）"
  [^Random rnd n]
  (let [l (ArrayList. ^java.util.Collection countries)]
    (Collections/shuffle l rnd)
    (vec (take n l))))

(defn gen-dataset
  "決済イベントを作る。各イベントは
     :card    カード番号
     :country 使われた国
     :ts      イベント時刻（実際に決済された時刻）
     :arr     到着時刻（Kafka に届いた時刻。遅延がなければ :ts と同じ）
   中身：
     普通のカード  cards 枚 × events-per-card 件。1 か国（1 割は途中で旅行して 2 か国）
     旅行者        tourists 枚。3 か国だが 20 分おき（5 分以内ではないので不正ではない）
     不正カード    fraud 枚。fraud-offsets 秒の間隔で、別々の国で 1 件ずつ
       straddle 枚は前半と後半をまたぐ（split-s の 90 秒前・30 秒前・30 秒後）
       delay-s > 0 なら、late-index 番目のイベントだけ delay-s 秒遅れて届く
   イベントは到着順（:arr）に並べて返す。"
  [{:keys [cards events-per-card fraud tourists straddle split-s duration-s
           fraud-offsets delay-s late-index seed]
    :or   {cards 1000 events-per-card 10 fraud 30 tourists 20 straddle 0
           split-s 1800 duration-s 3600 fraud-offsets [0 60 120]
           delay-s 0 late-index 1 seed 42}}]
  (let [rnd    (Random. (long seed))
        sec    (fn [s] (+ t0 (* s 1000) (.nextInt rnd 1000)))
        ev     (fn [card country ts arr kind]
                 {:card card :country country :ts ts :arr arr :kind kind})
        normal (vec (for [i (range cards)
                          :let [card          (card-id i)
                                [home abroad] (pick-countries rnd 2)
                                travel-at     (when (< (.nextDouble rnd) 0.1)
                                                (.nextInt rnd (int duration-s)))]
                          _ (range events-per-card)
                          :let [s  (.nextInt rnd (int duration-s))
                                ts (sec s)]]
                      (ev card (if (and travel-at (>= s travel-at)) abroad home) ts ts :normal)))
        tourist-cards (mapv #(card-id (+ cards %)) (range tourists))
        tourist-evs   (vec (for [card tourist-cards
                                 :let [cs (pick-countries rnd 3)
                                       s  (+ 60 (.nextInt rnd (int (- duration-s 2400))))]
                                 [k c] (map-indexed vector cs)
                                 :let [ts (sec (+ s (* k 1200)))]]
                             (ev card c ts ts :tourist)))
        fraud-cards    (mapv #(card-id (+ cards tourists %)) (range fraud))
        straddle-cards (set (take straddle fraud-cards))
        span           (last fraud-offsets)
        fraud-evs (vec (for [card fraud-cards
                             :let [cs (pick-countries rnd (count fraud-offsets))
                                   s0 (+ 300 (.nextInt rnd (int (- duration-s span 600))))
                                   s  (cond
                                        (straddle-cards card)              (- split-s 90)
                                        ;; またぎ以外が偶然 split をまたがないようにずらす
                                        (<= (- split-s span) s0 split-s)   (+ s0 300)
                                        :else                              s0)]
                             [k off] (map-indexed vector fraud-offsets)
                             :let [ts  (+ t0 (* (+ s off) 1000))
                                   arr (if (and (pos? delay-s) (= k late-index))
                                         (+ ts (* delay-s 1000))
                                         ts)]]
                         (ev card (nth cs k) ts arr :fraud)))
        events (->> (concat normal tourist-evs fraud-evs)
                    (sort-by :arr)
                    (map-indexed (fn [i e] (assoc e :id (format "pay-%06d" i))))
                    vec)]
    {:events         events
     :fraud-cards    (set fraud-cards)
     :straddle-cards straddle-cards
     :tourist-cards  (set tourist-cards)
     :split-ms       (+ t0 (* split-s 1000))}))

(defn encode [{:keys [id card country ts arr]}]
  (str/join "|" [id card country ts arr]))

(defn decode [^String v]
  (let [[id card country ts arr] (str/split v #"\|")]
    {:id id :card card :country country :ts (parse-long ts) :arr (parse-long arr)}))

(defn show-card
  "あるカードのイベントを、到着順にイベント時刻と到着時刻つきで表示する"
  [ds card]
  (println (format "%s のイベント（到着順）" card))
  (println "  国   イベント時刻  到着時刻")
  (doseq [{:keys [country ts arr]} (filter #(= card (:card %)) (:events ds))]
    (println (format "  %-4s %s      %s%s" country (hms ts) (hms arr)
                     (if (= ts arr) "" (format "  ← %d 秒遅れ" (quot (- arr ts) 1000)))))))

;; ============================================================
;; 検知ロジック（Kafka と無関係な純粋関数）
;; ============================================================

(defn detect?
  "entries = [[国 時刻] ...]。時刻 ts を含む長さ window-ms のどこかの区間に、
   3 か国以上がそろっているか。
   区間の始まりの候補は、ts - window-ms 〜 ts にある各エントリの時刻。"
  [entries ts]
  (boolean
   (some (fn [[_ s]]
           (when (<= (- ts window-ms) s ts)
             (>= (count (into #{}
                              (comp (filter (fn [[_ t]] (<= s t (+ s window-ms))))
                                    (map first))
                              entries))
                 3)))
         entries)))

(defn event-time
  ":processing → 到着時刻で判定、:event → イベント時刻で判定"
  [ev time-mode]
  (if (= time-mode :processing) (:arr ev) (:ts ev)))

(defn step
  "1 パーティション分の状態 pstate に、イベント ev を 1 件反映する。
   pstate = {:stream-time このパーティションで見た最大の時刻
             :cards {カード [[国 時刻] ...]}}
   horizon = stream-time - 5分 - lateness。これより古い時刻は
     ・状態から捨てる（もう判定に使わない）
     ・新しく届いても『遅すぎる』として捨てる
   戻り値 {:pstate 新しい状態 :result (:ok | :alert | :late) :entries そのカードのエントリ}"
  [pstate ev {:keys [time-mode lateness-ms]}]
  (let [ts      (event-time ev time-mode)
        st      (max (:stream-time pstate 0) ts)
        horizon (- st window-ms lateness-ms)
        card    (:card ev)]
    (if (< ts horizon)
      {:pstate (assoc pstate :stream-time st) :result :late :entries nil}
      (let [entries (->> (conj (get-in pstate [:cards card] []) [(:country ev) ts])
                         (filterv (fn [[_ t]] (>= t horizon))))]
        {:pstate  (-> pstate
                      (assoc :stream-time st)
                      (assoc-in [:cards card] entries))
         :result  (if (detect? entries ts) :alert :ok)
         :entries entries}))))

;; ============================================================
;; 結果の記録
;; ============================================================

(defonce stats (atom {}))
(defonce detected (atom {}))   ; カード → 最初に検知したときの情報

(defn reset-results! []
  (reset! stats {:processed 0 :alerts 0 :late 0 :changelog-writes 0
                 :restored-records 0 :restored-cards 0})
  (reset! detected {}))

(defn- bump! [k n] (swap! stats update k (fnil + 0) n))

(defn- fmt-entries [entries]
  (str/join " " (map (fn [[c t]] (str c "@" (hms t))) (sort-by second entries))))

;; ============================================================
;; Kafka：トピック・producer・consumer の設定
;; ============================================================

(defn- admin ^AdminClient [] (AdminClient/create {"bootstrap.servers" bootstrap}))

(defn create-topic! [topic partitions configs]
  (with-open [a (admin)]
    (-> (.createTopics a [(doto (NewTopic. ^String topic (int partitions) (short replication))
                            (.configs ^java.util.Map configs))])
        .all .get))
  (println "トピック作成:" topic (if (seq configs) configs ""))
  topic)

(defn new-run!
  "実験 1 回分のトピック（と changelog）と group 名を新しく作る"
  [label {:keys [changelog?]}]
  (let [id (str label "-" (mod (System/currentTimeMillis) 100000))]
    {:id        id
     :topic     (create-topic! (str "payments-" id) 3 {})
     :changelog (when changelog? (create-topic! (str "fraud-state-" id) 3 changelog-configs))
     :group     (str "fraud-" id)}))

(defn- producer-props []
  {"bootstrap.servers" bootstrap
   "key.serializer"    (.getName StringSerializer)
   "value.serializer"  (.getName StringSerializer)
   "acks"              "all"
   "linger.ms"         "5"})

(defn- consumer-props [group]
  {"bootstrap.servers"  bootstrap
   "group.id"           group
   "key.deserializer"   (.getName StringDeserializer)
   "value.deserializer" (.getName StringDeserializer)
   "enable.auto.commit" "false"
   "auto.offset.reset"  "earliest"
   "max.poll.records"   "500"})

(defn- plain-consumer-props
  "group に入らない consumer（changelog の復元・lag の計算用）"
  []
  {"bootstrap.servers"  bootstrap
   "key.deserializer"   (.getName StringDeserializer)
   "value.deserializer" (.getName StringDeserializer)
   "enable.auto.commit" "false"})

(defn send-events!
  "イベントを送る。keyless? が true ならキーなし（カード番号をキーにしない）。
   戻り値：カード → そのカードのイベントが入ったパーティションのベクタ"
  [topic events {:keys [keyless?]}]
  (with-open [p (KafkaProducer. ^java.util.Map (producer-props))]
    (let [futs (mapv (fn [e]
                       [e (.send p (ProducerRecord. ^String topic
                                                    (when-not keyless? (:card e))
                                                    (encode e)))])
                     events)]
      (.flush p)
      (println (format "送信: %d 件 → %s（%s）" (count events) topic
                       (if keyless? "キーなし" "キー = カード番号")))
      (reduce (fn [m [e f]] (update m (:card e) (fnil conj []) (.partition (.get f))))
              {} futs))))

;; ============================================================
;; changelog からの復元
;; ============================================================

(defn restore-partition
  "changelog の partition p を先頭から末尾まで読み、カードごとの最新の状態を作る。
   値が nil（tombstone）のキーは消す。
   戻り値 {:cards {カード entries} :records 読んだレコード数}"
  [changelog p]
  (with-open [c (KafkaConsumer. ^java.util.Map (plain-consumer-props))]
    (let [tp  (TopicPartition. ^String changelog (int p))
          tps [tp]]
      (.assign c tps)
      (.seekToBeginning c tps)
      (let [end (get (.endOffsets c tps) tp)]
        (loop [cards {} n 0]
          (if (>= (.position c tp) end)
            {:cards cards :records n}
            (let [[cards n] (reduce (fn [[m n] r]
                                      [(if (nil? (.value r))
                                         (dissoc m (.key r))
                                         (assoc m (.key r) (edn/read-string (.value r))))
                                       (inc n)])
                                    [cards n]
                                    (.poll c (Duration/ofMillis 200)))]
              (recur cards n))))))))

(defn changelog-stats
  "changelog 全体を読んで、レコード数とカード数（キーの種類）を数える"
  [changelog]
  (let [rs      (mapv #(restore-partition changelog %) (range 3))
        records (reduce + (map :records rs))
        cards   (reduce + (map (comp count :cards) rs))]
    (println (format "changelog %s: レコード %d 件 / カード %d 枚" changelog records cards))
    {:records records :cards cards}))

;; ============================================================
;; worker（consumer 1 台 = スレッド 1 本）
;; ============================================================

(defn- handle-record! [wname state prod changelog opts r]
  (let [p  (.partition r)
        ev (decode (.value r))
        {:keys [pstate result entries]} (step (get @state p {:stream-time 0 :cards {}}) ev opts)]
    (swap! state assoc p pstate)
    (bump! :processed 1)
    (if (= result :late)
      (bump! :late 1)
      (do
        ;; 状態が変わったら changelog に書く（キー = カード、パーティション = 元と同じ番号）
        (when changelog
          (.send prod (ProducerRecord. ^String changelog (Integer/valueOf (int p))
                                       (:card ev) (pr-str entries)))
          (bump! :changelog-writes 1))
        (when (= result :alert)
          (bump! :alerts 1)
          (let [card     (:card ev)
                [old _]  (swap-vals! detected
                                     (fn [m] (if (contains? m card)
                                               m
                                               (assoc m card {:worker wname :partition p
                                                              :entries entries}))))]
            (when-not (contains? old card)
              (println (format "[%s] ALERT %s (p%d) %s" wname card p (fmt-entries entries))))))))))

(defn- listener
  "リバランスのときに呼ばれる。
   changelog あり：取り上げられたパーティションの状態を捨て、割り当てられたら changelog から復元
   changelog なし：何もしない（状態はメモリに残ったまま／新しいパーティションは空から）"
  [wname state assigned changelog prod]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ parts]
      (let [ps (vec (sort (map #(.partition ^TopicPartition %) parts)))]
        (swap! assigned #(reduce disj % ps))
        (when (seq ps)
          (when prod (.flush ^KafkaProducer prod))
          (if changelog
            (do (swap! state #(apply dissoc % ps))
                (println (format "[%s] 取り上げ: %s（状態を捨てる）" wname ps)))
            (println (format "[%s] 取り上げ: %s" wname ps))))))
    (onPartitionsAssigned [_ parts]
      (let [ps (vec (sort (map #(.partition ^TopicPartition %) parts)))]
        (println (format "[%s] 割り当て: %s" wname ps))
        (swap! assigned into ps)
        (doseq [p ps]
          (if changelog
            (let [t0' (System/nanoTime)
                  {:keys [cards records]} (restore-partition changelog p)
                  st  (reduce max 0 (for [[_ es] cards [_ ts] es] ts))]
              (swap! state assoc p {:stream-time st :cards cards})
              (bump! :restored-records records)
              (bump! :restored-cards (count cards))
              (println (format "[%s]   p%d を復元: %d 件読んで %d 枚分 (%.0f ms)"
                               wname p records (count cards)
                               (/ (- (System/nanoTime) t0') 1e6))))
            (let [n (count (get-in @state [p :cards]))]
              (swap! state update p #(or % {:stream-time 0 :cards {}}))
              (println (format "[%s]   p%d の手元の状態: %d 枚分" wname p n)))))))))

(defn- worker-loop [wname {:keys [topic group changelog] :as opts} running state assigned]
  (let [c    (KafkaConsumer. ^java.util.Map (consumer-props group))
        prod (when changelog (KafkaProducer. ^java.util.Map (producer-props)))]
    (try
      (.subscribe c [topic] (listener wname state assigned changelog prod))
      (while @running
        (let [recs (.poll c (Duration/ofMillis 200))]
          (doseq [r recs] (handle-record! wname state prod changelog opts r))
          (when-not (.isEmpty recs)
            ;; changelog を書き切ってから commit（Day 5 の展開と同じ順番）
            (when prod (.flush prod))
            (.commitSync c))))
      (catch Exception e
        (println (format "[%s] 例外: %s" wname e)))
      (finally
        (when prod (.close prod))
        (.close c)
        (println (format "[%s] 停止" wname))))))

(defonce workers (atom {}))

(defn start-worker!
  "opts = {:topic :group :changelog（なければ nil） :time-mode :lateness-ms}"
  [wname opts]
  (let [running  (atom true)
        state    (atom {})       ; パーティション → そのパーティションの状態
        assigned (atom #{})      ; いま割り当てられているパーティション
        t        (doto (Thread. ^Runnable #(worker-loop wname opts running state assigned))
                   (.setName wname)
                   (.start))]
    (swap! workers assoc wname {:running running :thread t :state state :assigned assigned})
    wname))

(defn start-workers! [names opts]
  (doseq [n names] (start-worker! n opts)))

(defn stop-worker! [wname]
  (when-let [{:keys [running ^Thread thread]} (get @workers wname)]
    (reset! running false)
    (.join thread)
    (swap! workers dissoc wname)))

(defn stop-all!
  "全 worker を同時に止める（1 台ずつ止めると、その間にリバランスが起きるため）"
  []
  (let [ws @workers]
    (doseq [[_ {:keys [running]}] ws] (reset! running false))
    (doseq [[_ {:keys [^Thread thread]}] ws] (.join thread))
    (reset! workers {})))

(defn worker-status
  "各 worker の {:assigned 割り当て中のパーティション
                 :state {パーティション 状態に入っているカード枚数}}"
  []
  (into (sorted-map)
        (for [[n {:keys [state assigned]}] @workers]
          [n {:assigned (vec (sort @assigned))
              :state    (into (sorted-map) (for [[p ps] @state] [p (count (:cards ps))]))}])))

(defn wait-assigned!
  "全 worker にパーティションが割り当てられ、3 つとも誰かが担当している状態になるまで待つ
   （送り始める前にリバランスを終わらせておくため）"
  [& {:keys [timeout-s] :or {timeout-s 60}}]
  (let [start (System/currentTimeMillis)]
    (loop []
      (let [as (map (comp deref :assigned) (vals @workers))]
        (cond
          (and (seq as) (every? seq as) (= 3 (count (apply concat as))))
          (println "割り当て完了:" (into (sorted-map) (for [[n w] @workers] [n (vec (sort @(:assigned w)))])))

          (> (- (System/currentTimeMillis) start) (* 1000 timeout-s))
          (println "割り当て待ちタイムアウト")

          :else (do (Thread/sleep 200) (recur)))))))

;; ============================================================
;; 完了待ちと集計
;; ============================================================

(defn group-lag [group topic]
  (with-open [a (admin)
              c (KafkaConsumer. ^java.util.Map (plain-consumer-props))]
    (let [tps       (mapv #(TopicPartition. ^String topic (int (.partition %))) (.partitionsFor c topic))
          ends      (.endOffsets c tps)
          committed (-> a (.listConsumerGroupOffsets ^String group) .partitionsToOffsetAndMetadata .get)]
      (reduce + (for [tp tps]
                  (- (get ends tp) (if-let [om (get committed tp)] (.offset om) 0)))))))

(defn wait-done!
  "group の lag が 0 になるまで待つ"
  [{:keys [topic group]} & {:keys [timeout-s] :or {timeout-s 120}}]
  (let [start (System/currentTimeMillis)]
    (loop []
      (let [l       (group-lag group topic)
            elapsed (/ (- (System/currentTimeMillis) start) 1000.0)]
        (cond
          (zero? l)           (println (format "処理完了（lag 0、%.1f 秒）" elapsed))
          (> elapsed timeout-s) (println "タイムアウト。lag =" l)
          :else               (do (Thread/sleep 500) (recur)))))))

(defn report
  "検知結果を正解と照らし合わせる"
  [{:keys [fraud-cards straddle-cards tourist-cards]} & {:keys [partitions]}]
  (let [det  (set (keys @detected))
        miss (sort (remove det fraud-cards))
        s    @stats
        res  (cond-> {:fraud          (count fraud-cards)
                      :detected       (count (filter det fraud-cards))
                      :missed         (count miss)
                      :false-positive (count (remove fraud-cards det))
                      :tourist-alerts (count (filter tourist-cards det))
                      :processed      (:processed s)
                      :late-dropped   (:late s)}
               (seq straddle-cards)
               (assoc :straddle          (count straddle-cards)
                      :straddle-detected (count (filter det straddle-cards)))
               (pos? (:changelog-writes s))
               (assoc :changelog-writes (:changelog-writes s)
                      :restored-records (:restored-records s)
                      :restored-cards   (:restored-cards s)))]
    (doseq [c (take 12 miss)]
      (println (format "  見逃し: %s%s%s" c
                       (if (straddle-cards c) "（またぎ）" "")
                       (if partitions (str " パーティション " (get partitions c)) ""))))
    (when (> (count miss) 12) (println "  ...ほか" (- (count miss) 12) "枚"))
    res))

;; ============================================================
;; 実験
;; ============================================================

(def worker-names ["w1" "w2" "w3"])

(defn run-basic!
  "Step 1・2：consumer 3 台で 1 回流して検知する。
   keyless? true でキーなし送信（Step 2）。"
  [label & {:keys [keyless?]}]
  (stop-all!)
  (reset-results!)
  (let [ds   (gen-dataset {})
        run  (new-run! label {})
        opts (assoc run :time-mode :event :lateness-ms 0)]
    (start-workers! worker-names opts)
    (wait-assigned!)
    (let [parts (send-events! (:topic run) (:events ds) {:keyless? keyless?})]
      (wait-done! run)
      (let [r (report ds :partitions parts)]
        (stop-all!)
        r))))

(defn run-restart!
  "Step 3：前半を流す → 再起動（:all 全台入れ替え / :one 1 台だけ停止）→ 後半を流す。
   changelog? true なら状態を changelog に書き、割り当て時に復元する。"
  [label & {:keys [changelog? restart] :or {restart :all}}]
  (stop-all!)
  (reset-results!)
  (let [ds        (gen-dataset {:straddle 10})
        run       (new-run! label {:changelog? changelog?})
        opts      (assoc run :time-mode :event :lateness-ms 0)
        [ph1 ph2] ((juxt filter remove) #(< (:arr %) (:split-ms ds)) (:events ds))]
    (start-workers! worker-names opts)
    (wait-assigned!)
    (println (format "=== 前半（〜%s）" (hms (:split-ms ds))))
    (let [p1 (send-events! (:topic run) ph1 {})]
      (wait-done! run)
      (println "=== 前半終了時点の状態" (worker-status))
      (println "=== 再起動:" restart)
      (case restart
        :all (do (stop-all!) (start-workers! ["w4" "w5" "w6"] opts))
        :one (stop-worker! "w1"))
      (wait-assigned!)
      (println (format "=== 後半（%s〜）" (hms (:split-ms ds))))
      (let [p2 (send-events! (:topic run) ph2 {})]
        (wait-done! run)
        (let [r (report ds :partitions (merge-with into p1 p2))]
          (when changelog? (changelog-stats (:changelog run)))
          (stop-all!)
          (assoc r :changelog (:changelog run)))))))

(defn run-late!
  "Step 4：不正カードの 2 件目だけ delay-s 秒遅れて届く。
   time-mode :processing（到着時刻で判定）/ :event（イベント時刻で判定）
   lateness-s：遅れをどこまで待つか（状態を 5分 + lateness-s 残す）"
  [label & {:keys [time-mode lateness-s delay-s] :or {time-mode :event lateness-s 0 delay-s 300}}]
  (stop-all!)
  (reset-results!)
  (let [ds   (gen-dataset {:fraud-offsets [0 120 240] :delay-s delay-s :late-index 1})
        run  (new-run! label {})
        opts (assoc run :time-mode time-mode :lateness-ms (* 1000 lateness-s))]
    (println (format "=== 判定: %s、許容遅延 %d 秒、遅延 %d 秒"
                     (if (= time-mode :processing) "到着時刻" "イベント時刻") lateness-s delay-s))
    (show-card ds (first (sort (:fraud-cards ds))))
    (start-workers! worker-names opts)
    (wait-assigned!)
    (send-events! (:topic run) (:events ds) {})
    (wait-done! run)
    (let [r (report ds)]
      (stop-all!)
      r)))
