(ns kafka-study.day3.sensors
  (:require [clojure.string :as str])
  (:import [java.time Duration]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.common TopicPartition PartitionInfo]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(def bootstrap "localhost:9092,localhost:9094,localhost:9096")

;; =========================================================
;; センサーのデータ
;; =========================================================

(defn sensor-id [i]
  (format "sensor-%03d" i))

(defn make-reading
  "センサー1件の値。形式は センサーID|通し番号|時刻ms|温度"
  [sid seq-no]
  (format "%s|%d|%d|%.1f" sid seq-no (System/currentTimeMillis) (+ 20.0 (rand 10.0))))

(defn parse-reading [v]
  (let [[sid seq-no ts temp] (str/split v #"\|")]
    {:sensor sid
     :seq    (parse-long seq-no)
     :ts     (parse-long ts)
     :temp   (parse-double temp)}))

;; =========================================================
;; 送る
;; =========================================================

(defn make-producer []
  (KafkaProducer. ^java.util.Map {"bootstrap.servers" bootstrap
                                  "linger.ms"         "10"}
                  (StringSerializer.)
                  (StringSerializer.)))

(defn send-round-robin!
  "n-sensors 個のセンサーが、それぞれ per-sensor 回ずつ値を送る。
   1周ごとに全センサーが1件ずつ送る。keyed? が false ならキーを付けない。"
  [^KafkaProducer p topic n-sensors per-sensor {:keys [keyed?] :or {keyed? true}}]
  (dotimes [s per-sensor]
    (dotimes [i n-sensors]
      (let [sid (sensor-id i)]
        (.send p (ProducerRecord. topic (when keyed? sid) (make-reading sid s))))))
  (.flush p))

;; =========================================================
;; 読んで調べる
;; =========================================================

(defn read-all
  "トピックの全パーティションを先頭から末尾まで読み、
   [{:partition :offset :sensor :seq ...}] を返す。"
  [topic]
  (with-open [c (KafkaConsumer. ^java.util.Map {"bootstrap.servers"  bootstrap
                                                "enable.auto.commit" "false"}
                                (StringDeserializer.)
                                (StringDeserializer.))]
    (let [tps (mapv (fn [^PartitionInfo pi] (TopicPartition. topic (.partition pi)))
                    (.partitionsFor c topic))
          _   (.assign c tps)
          _   (.seekToBeginning c tps)
          end (.endOffsets c tps)]
      (loop [acc (transient [])]
        (if (every? (fn [^TopicPartition tp] (>= (.position c tp) (get end tp))) tps)
          (persistent! acc)
          (recur (reduce (fn [a ^ConsumerRecord r]
                           (conj! a (assoc (parse-reading (.value r))
                                           :partition (.partition r)
                                           :offset    (.offset r))))
                         acc
                         (.poll c (Duration/ofMillis 500)))))))))

(defn check-keys
  "センサーごとに、(1) 全部が同じパーティションに入っているか、
   (2) offset 順に並べたとき通し番号が昇順か、を調べる。"
  [records]
  (let [by-sensor (group-by :sensor records)
        single?   (fn [rs] (= 1 (count (distinct (map :partition rs)))))
        ordered?  (fn [rs] (apply < (map :seq (sort-by :offset rs))))]
    {:sensors               (count by-sensor)
     :records               (count records)
     :all-single-partition? (every? single? (vals by-sensor))
     :all-in-order?         (every? ordered? (vals by-sensor))
     :records-per-partition (into (sorted-map) (frequencies (map :partition records)))
     :sensors-per-partition (into (sorted-map)
                                  (frequencies (for [rs (vals by-sensor)
                                                     :when (single? rs)]
                                                 (:partition (first rs)))))}))
