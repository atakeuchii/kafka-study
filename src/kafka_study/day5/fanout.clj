(ns kafka-study.day5.fanout
  (:require [clojure.string :as str])
  (:import [java.time Duration]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition PartitionInfo]
           [org.apache.kafka.common.header Header]
           [org.apache.kafka.common.utils Utils]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(def bootstrap "localhost:9092,localhost:9094,localhost:9096")

;; =========================================================
;; ユーザーとフォロワー
;; =========================================================

(def celeb "u000")
(def celeb-followers 30000)
(def normal-followers 50)

(defn user-id [i] (format "u%03d" i))

(defn followers-of
  "フォロワーID（整数）の列。人気ユーザーは 30,000 人、それ以外は 50 人。"
  [user]
  (if (= user celeb)
    (range celeb-followers)
    (let [r (java.util.Random. (hash user))]
      (repeatedly normal-followers #(.nextInt r 1000000)))))

(defn partition-of
  "キーがどのパーティションに入るか（Kafka の producer と同じ計算）。"
  [^String k n]
  (mod (Utils/toPositive (Utils/murmur2 (.getBytes k "UTF-8"))) n))

(defn pick-users
  "人気ユーザーと同じパーティションに入るユーザーと、違うパーティションに入るユーザーを1人ずつ選ぶ。"
  [n-partitions]
  (let [p      (partition-of celeb n-partitions)
        others (map user-id (range 1 1000))]
    {:same  (first (filter #(= p (partition-of % n-partitions)) others))
     :other (first (filter #(not= p (partition-of % n-partitions)) others))}))

;; =========================================================
;; 投稿
;; =========================================================

(defn make-producer []
  (KafkaProducer. ^java.util.Map {"bootstrap.servers" bootstrap "linger.ms" "5"}
                  (StringSerializer.)
                  (StringSerializer.)))

(defn post!
  "投稿イベントを送る。値は 投稿ID|ユーザー|投稿時刻ms、キーはユーザー。"
  [^KafkaProducer p topic user post-id]
  @(.send p (ProducerRecord. topic user (str/join "|" [post-id user (System/currentTimeMillis)]))))

(defn post-broken!
  "壊れた投稿（poison message）を送る。"
  [^KafkaProducer p topic user]
  @(.send p (ProducerRecord. topic user "BROKEN")))

(defn parse-post
  "投稿イベントを読む。壊れていたら例外を投げる。"
  [v]
  (let [[post-id user created] (str/split v #"\|")
        created' (some-> created parse-long)]
    (when (or (str/blank? user) (nil? created'))
      (throw (ex-info (str "壊れた投稿: " v) {:value v})))
    {:post-id post-id :user user :created created'}))

;; =========================================================
;; 通知を送った記録
;; =========================================================

(defonce results (atom {}))   ; 投稿ID → {:user :created :first-sent :last-sent :sent}

(defn- record-sent!
  "投稿 post-id の通知を n 件送ったことを記録する。"
  [post-id user created n]
  (let [now (System/currentTimeMillis)]
    (swap! results update post-id
           (fn [r] (-> (or r {:user user :created created :sent 0})
                       (update :first-sent #(or % now))
                       (assoc :last-sent now)
                       (update :sent + n))))))

(defn report
  "投稿ごとに、投稿から最初の通知まで（:wait-ms）と、最後の通知まで（:done-ms）の時間を返す。"
  []
  (into (sorted-map)
        (for [[post-id {:keys [user created first-sent last-sent sent]}] @results]
          [post-id {:user    user
                    :sent    sent
                    :wait-ms (- first-sent created)
                    :done-ms (- last-sent created)}])))

;; =========================================================
;; Step 1：1段構成
;; =========================================================

(defn- send-notifications!
  "フォロワー全員に通知を送る（1件 1ms の代わりに、100件ごとに 100ms 待つ）。"
  [{:keys [post-id user created]}]
  (doseq [chunk (partition-all 100 (followers-of user))]
    (Thread/sleep (long (count chunk)))
    (record-sent! post-id user created (count chunk))))

(defn single-stage-handler [^ConsumerRecord r]
  (send-notifications! (parse-post (.value r))))

;; =========================================================
;; Step 2：2段構成
;; =========================================================

(defn make-expander
  "展開する段の batch-handler を返す。
   投稿を受け取り、フォロワー1人につき1件の通知メッセージを out-topic に produce する。"
  [out-topic]
  (let [p (make-producer)]
    (fn [recs]
      (doseq [^ConsumerRecord r recs
              :let [{:keys [post-id user created]} (parse-post (.value r))]
              f (followers-of user)]
        (.send p (ProducerRecord. out-topic (str f) (str/join "|" [post-id user created f]))))
      ;; commit の前に、produce した通知を全部 Kafka に届けておく
      (.flush p))))

(defn parse-notification [v]
  (let [[post-id user created _follower] (str/split v #"\|")]
    {:post-id post-id :user user :created (parse-long created)}))

(defn sender-handler
  "送信する段の batch-handler。受け取った通知をまとめて送る（1件 1ms の代わりに、件数 ms 待つ）。"
  [recs]
  (Thread/sleep (long (count recs)))
  (doseq [[post-id ns] (group-by :post-id (map #(parse-notification (.value ^ConsumerRecord %)) recs))]
    (let [{:keys [user created]} (first ns)]
      (record-sent! post-id user created (count ns)))))

;; =========================================================
;; Step 3：壊れたメッセージの扱い方
;; =========================================================

(defonce abort-retries? (atom false))   ; stop-all! のときに、リトライのループを抜けるため

(defn- dlt-record
  "元のメッセージに、エラーの内容と元の場所をヘッダとして付けたレコードを作る。"
  [dlt-topic ^ConsumerRecord r ^Exception e]
  (let [rec (ProducerRecord. dlt-topic (.key r) (.value r))]
    (doto (.headers rec)
      (.add "error"  (.getBytes (str (ex-message e)) "UTF-8"))
      (.add "source" (.getBytes (str (.topic r) "-" (.partition r) "@" (.offset r)) "UTF-8")))
    rec))

(defn make-safe-handler
  "壊れたメッセージの扱い方を mode で指定した、1段構成の handler を返す。
   :retry-forever … 成功するまで1秒おきにやり直す
   :skip          … 例外を握りつぶして次へ進む
   :crash         … 例外をそのまま投げる（consumer ごと止まる）
   :dlt           … max-attempts 回やり直してもだめなら dlt-topic に送って次へ進む"
  [mode & {:keys [dlt-topic max-attempts] :or {max-attempts 3}}]
  (let [p (when (= mode :dlt) (make-producer))]
    (fn [^ConsumerRecord r]
      (case mode
        :crash
        (single-stage-handler r)

        :skip
        (try (single-stage-handler r)
             (catch Exception e
               (println (str "[skip] " (ex-message e) "（offset " (.offset r) "）"))))

        :retry-forever
        (loop []
          (let [ok? (try (single-stage-handler r) true
                         (catch Exception e
                           (println (str "[retry] " (ex-message e) " → 1秒後にやり直す"))
                           false))]
            (when-not (or ok? @abort-retries?)
              (Thread/sleep 1000)
              (recur))))

        :dlt
        (loop [attempt 1]
          (let [err (try (single-stage-handler r) nil
                         (catch Exception e e))]
            (cond
              (nil? err)
              :ok

              (< attempt max-attempts)
              (do (println (str "[retry " attempt "/" max-attempts "] " (ex-message err)))
                  (Thread/sleep 500)
                  (recur (inc attempt)))

              :else
              (do @(.send ^KafkaProducer p (dlt-record dlt-topic r err))
                  (println (str "[dlt] " (ex-message err) " → " dlt-topic " に送った"))))))))))

(defn read-dlt
  "dead letter のトピックを先頭から全部読み、メッセージとヘッダを返す。"
  [topic]
  (with-open [c (KafkaConsumer. ^java.util.Map {"bootstrap.servers" bootstrap "enable.auto.commit" "false"}
                                (StringDeserializer.)
                                (StringDeserializer.))]
    (let [tps (mapv (fn [^PartitionInfo pi] (TopicPartition. topic (.partition pi)))
                    (.partitionsFor c topic))
          _   (.assign c tps)
          _   (.seekToBeginning c tps)
          end (.endOffsets c tps)]
      (loop [acc []]
        (if (every? (fn [^TopicPartition tp] (>= (.position c tp) (get end tp))) tps)
          acc
          (recur (into acc
                       (for [^ConsumerRecord r (.poll c (Duration/ofMillis 500))]
                         {:key     (.key r)
                          :value   (.value r)
                          :headers (into {} (for [^Header h (.headers r)]
                                              [(.key h) (String. (.value h) "UTF-8")]))}))))))))

;; =========================================================
;; consumer の起動と停止
;; =========================================================

(defn- worker-loop
  "poll で受け取ったメッセージをまとめて batch-handler に渡し、終わったら commit する。"
  [id topic group batch-handler props stop?]
  (try
    (with-open [c (KafkaConsumer. ^java.util.Map (merge {"bootstrap.servers"  bootstrap
                                                         "group.id"           group
                                                         "auto.offset.reset"  "earliest"
                                                         "enable.auto.commit" "false"
                                                         "max.poll.records"   "10"}
                                                        props)
                                  (StringDeserializer.)
                                  (StringDeserializer.))]
      (.subscribe c [topic])
      (while (not @stop?)
        (let [recs (.poll c (Duration/ofMillis 200))]
          (when-not (.isEmpty recs)
            (batch-handler (vec recs))
            (.commitSync c)))))
    (catch Exception e
      (println (str "[" id "] 止まった: " (ex-message e))))))

(defonce workers (atom {}))

(defn start-batch-workers!
  "同じグループの consumer を n 台起動する。batch-handler は、poll で受け取ったメッセージのベクタを受け取る。"
  [prefix n topic group batch-handler props]
  (doseq [i (range 1 (inc n))
          :let [id (str prefix "-" i)]]
    (let [stop? (atom false)
          fut   (future (worker-loop id topic group batch-handler props stop?))]
      (swap! workers assoc id {:stop? stop? :fut fut}))))

(defn start-workers!
  "1件ずつ handler に渡す版。"
  [prefix n topic group handler]
  (start-batch-workers! prefix n topic group
                        (fn [recs] (doseq [r recs] (handler r)))
                        {}))

(defn worker-status
  "各 consumer が動いているか（:running）、止まっているか（:stopped）。"
  []
  (into (sorted-map)
        (for [[id {:keys [fut]}] @workers]
          [id (if (realized? fut) :stopped :running)])))

(defn stop-all! []
  (reset! abort-retries? true)
  (doseq [[_ {:keys [stop?]}] @workers] (reset! stop? true))
  (doseq [[_ {:keys [fut]}] @workers] @fut)
  (reset! workers {})
  (reset! abort-retries? false))

;; =========================================================
;; シナリオ
;; =========================================================

(defn run-scenario!
  "人気ユーザー → Aと同じパーティションのユーザー → 違うパーティションのユーザー、の順に投稿する。
   c-delay-ms を指定すると、post-C をその時間だけ遅らせて投稿する。"
  [topic n-partitions & {:keys [c-delay-ms] :or {c-delay-ms 0}}]
  (reset! results {})
  (let [{:keys [same other]} (pick-users n-partitions)]
    (with-open [p (make-producer)]
      (post! p topic celeb "post-A")
      (post! p topic same  "post-B")
      (when (pos? c-delay-ms) (Thread/sleep (long c-delay-ms)))
      (post! p topic other "post-C"))
    {:A celeb :B same :C other
     :partitions {:A (partition-of celeb n-partitions)
                  :B (partition-of same n-partitions)
                  :C (partition-of other n-partitions)}}))

(defn run-poison-scenario!
  "Bのユーザーのキーで壊れた投稿を送り、そのあと同じユーザーの普通の投稿（post-B2）と、
   違うパーティションのユーザーの投稿（post-C2）を送る。"
  [topic n-partitions]
  (reset! results {})
  (let [{:keys [same other]} (pick-users n-partitions)]
    (with-open [p (make-producer)]
      (post-broken! p topic same)
      (post! p topic same  "post-B2")
      (post! p topic other "post-C2"))
    {:poison-key same :B2 same :C2 other
     :partitions {:poison (partition-of same n-partitions)
                  :C2     (partition-of other n-partitions)}}))
