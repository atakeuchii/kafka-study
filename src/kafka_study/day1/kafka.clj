(ns kafka-study.day1.kafka
  (:require [kafka-study.day1.app :as app]
            [kafka-study.day1.receivers :as r])
  (:import [java.time Duration]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(def bootstrap "localhost:9092")
(def topic "app-logs")

;; =========================================================
;; アプリ側（producer）
;; =========================================================

(defn make-producer []
  (KafkaProducer. ^java.util.Map {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap}
                  (StringSerializer.)
                  (StringSerializer.)))

(defn log!
  "Kafka 版。トピックに送るだけ。受け手のことは知らない。"
  [^KafkaProducer p line]
  (.send p (ProducerRecord. topic line)))

(defn run-app!
  "direct/run-app! と同じ測り方で、log! だけ Kafka 版に替えたもの。"
  [p n]
  (let [start  (System/nanoTime)
        result (reduce
                (fn [acc _]
                  (let [line (app/make-line)
                        t0   (System/nanoTime)
                        ok?  (try (log! p line) true
                                  (catch Exception _ false))
                        ms   (/ (- (System/nanoTime) t0) 1e6)]
                    (cond-> (update acc :max-log-ms max ms)
                      (not ok?) (update :app-failed inc))))
                {:max-log-ms 0.0 :app-failed 0}
                (range n))]
    (assoc result
           :n n
           :total-ms (/ (- (System/nanoTime) start) 1e6))))

;; =========================================================
;; 受け手側（consumer）
;; =========================================================

;; グループ名 → そのグループが呼ぶ受け手
(def receivers
  {"file-writer" r/write-file!
   "db-writer"   r/write-db!
   "alerter"     r/check-alert!})

(defn- make-consumer [group]
  (KafkaConsumer. ^java.util.Map {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG  bootstrap
                                  ConsumerConfig/GROUP_ID_CONFIG           group
                                  ConsumerConfig/AUTO_OFFSET_RESET_CONFIG  "earliest"
                                  ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false"}
                  (StringDeserializer.)
                  (StringDeserializer.)))

(defn- handle-with-retry!
  "1行を受け手に渡す。失敗したら1秒待って同じ行をやり直す。
   成功したら true、途中で停止を指示されたら false を返す。"
  [group handler line stop?]
  (loop []
    (cond
      @stop? false
      (try (handler line) true
           (catch Exception e
             (println (str "[" group "] " (ex-message e) " → 1秒後にリトライ"))
             false)) true
      :else (do (Thread/sleep 1000) (recur)))))

(defn- consume-loop
  "poll → 全件処理 → commit を、停止を指示されるまで繰り返す。"
  [group handler stop?]
  (let [c (make-consumer group)]
    (try
      (.subscribe c [topic])
      (loop []
        (when-not @stop?
          (let [recs  (.poll c (Duration/ofMillis 500))
                done? (every? #(handle-with-retry! group handler (.value %) stop?) recs)]
            (when (and done? (not (.isEmpty recs)))
              (.commitSync c))
            (recur))))
      (catch Exception e
        (println (str "[" group "] consumer error: " e)))
      (finally
        (.close c)))))

;; ---- 起動と停止 ----

(defonce consumers (atom {}))   ; グループ名 → {:stop? atom, :fut future}

(defn start-consumer! [group]
  (when-not (contains? @consumers group)
    (let [stop? (atom false)
          fut   (future (consume-loop group (receivers group) stop?))]
      (swap! consumers assoc group {:stop? stop? :fut fut})
      group)))

(defn stop-consumer! [group]
  (when-let [{:keys [stop? fut]} (get @consumers group)]
    (reset! stop? true)
    @fut                        ; ループが終わって close されるまで待つ
    (swap! consumers dissoc group)
    group))

(defn start-all! [] (doseq [g (keys receivers)] (start-consumer! g)))
(defn stop-all!  [] (doseq [g (keys @consumers)] (stop-consumer! g)))

(comment
  (require '[kafka-study.day1.receivers :as r]
           '[kafka-study.day1.kafka :as k] :reload-all)

  ;; 3つの consumer を起動して、producer を作る
  (k/start-all!)
  (def p (k/make-producer))

  ;; ---- 実験A：正常時 ----
  (r/reset-all!)
  (k/run-app! p 1000)
  ;; 数秒待ってから
  (r/counts)

  ;; ---- 実験B：DB が遅い（1回 200ms） ----
  (r/reset-all!)
  (swap! r/faults assoc-in [:db :delay-ms] 200)
  (k/run-app! p 100)
  (r/counts)          ; すぐに実行
  ;; 3秒ほど待ってから、もう一度
  (r/counts)

  ;; ---- 実験C：DB が落ちている ----
  (r/reset-all!)
  (swap! r/faults assoc-in [:db :down?] true)
  (k/run-app! p 100)
  ;; 数秒待ってから
  (r/counts)

  ;; DB を復旧させる
  (swap! r/faults assoc-in [:db :down?] false)
  ;; 数秒待ってから
  (r/counts)

  )