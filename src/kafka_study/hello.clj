(ns kafka-study.hello
  (:import [java.time Duration]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(def bootstrap "localhost:9092")

;; ---- producer ----

(defn make-producer
  "キーも値も文字列で送る producer を作る。"
  []
  (KafkaProducer. ^java.util.Map {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap}
                  (StringSerializer.)
                  (StringSerializer.)))

(defn send!
  "1件送る。戻り値は Future（deref すると送信完了まで待ち、RecordMetadata が返る）。"
  [^KafkaProducer producer ^String topic k v]
  (.send producer (ProducerRecord. topic k v)))

;; ---- consumer ----

(defn make-consumer
  "group に属する consumer を作る。commit の記録がなければ先頭から読む。"
  [group]
  (KafkaConsumer. ^java.util.Map {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap
                                  ConsumerConfig/GROUP_ID_CONFIG group
                                  ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"}
                  (StringDeserializer.)
                  (StringDeserializer.)))

(defn poll-once
  "最大2秒待ってメッセージを取り、map のベクタにして返す。"
  [^KafkaConsumer consumer]
  (mapv (fn [r] {:partition (.partition r)
                 :offset    (.offset r)
                 :key       (.key r)
                 :value     (.value r)})
        (.poll consumer (Duration/ofSeconds 2))))
