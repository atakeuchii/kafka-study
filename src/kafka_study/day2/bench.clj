(ns kafka-study.day2.bench
  (:require [kafka-study.day2.clicks :as clicks])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig Callback]
           [org.apache.kafka.common Metric MetricName]
           [org.apache.kafka.common.serialization StringSerializer]))

(def default-config
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"})

(defn make-producer
  "overrides で設定を上書きした producer を作る。"
  ([] (make-producer {}))
  ([overrides]
   (KafkaProducer. ^java.util.Map (merge default-config overrides)
                   (StringSerializer.)
                   (StringSerializer.))))

(defn- elapsed-ms [start]
  (/ (- (System/nanoTime) start) 1e6))

(defn- summarize [n ms]
  {:n            n
   :ms           (Math/round (double ms))
   :msgs-per-sec (Math/round (/ n (/ ms 1000.0)))
   :ms-per-msg   (/ (Math/round (* 1000.0 (/ ms n))) 1000.0)})

(defn- warm-up!
  "1件送って、トピックの情報の取得などを済ませておく（計測に含めないため）。"
  [^KafkaProducer p topic]
  @(.send p (ProducerRecord. topic "warmup" "warmup")))

;; ---- 同期送信 ----

(defn run-sync!
  "n 件を同期送信する（1件ごとに @ で送信完了を待つ）。"
  [^KafkaProducer p topic n]
  (warm-up! p topic)
  (let [start (System/nanoTime)]
    (dotimes [_ n]
      (let [[k v] (clicks/make-event)]
        @(.send p (ProducerRecord. topic k v))))
    (summarize n (elapsed-ms start))))

;; ---- 非同期送信 ----

(defn- counting-callback
  "送信結果が返ってきたら、成功／失敗を数えるコールバック。"
  [^AtomicLong ok ^AtomicLong failed]
  (reify Callback
    (onCompletion [_ _ ex]
      (if ex
        (.incrementAndGet failed)
        (.incrementAndGet ok)))))

(defn metric
  "producer の統計値（producer-metrics グループ）を名前で取り出す。"
  [^KafkaProducer p metric-name]
  (some (fn [[^MetricName mn ^Metric m]]
          (when (and (= metric-name (.name mn))
                     (= "producer-metrics" (.group mn)))
            (.metricValue m)))
        (.metrics p)))

(defn- round1 [x]
  (when (number? x) (/ (Math/round (* 10.0 (double x))) 10.0)))

(defn run-async!
  "overrides の設定で producer を新しく作り、n 件を非同期送信する。
   全件の送信結果が返ってくる（flush が終わる）までの時間を測る。"
  [overrides topic n]
  (with-open [p (make-producer overrides)]
    (warm-up! p topic)
    (let [ok     (AtomicLong.)
          failed (AtomicLong.)
          cb     (counting-callback ok failed)
          start  (System/nanoTime)]
      (dotimes [_ n]
        (let [[k v] (clicks/make-event)]
          (.send p (ProducerRecord. topic k v) cb)))
      (.flush p)
      (merge (summarize n (elapsed-ms start))
             {:ok                      (.get ok)
              :failed                  (.get failed)
              :batch-size-avg          (round1 (metric p "batch-size-avg"))
              :records-per-request-avg (round1 (metric p "records-per-request-avg"))
              :compression-rate-avg    (round1 (metric p "compression-rate-avg"))}))))
