(ns kafka-study.day4.outbox
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [kafka-study.day4.orders :as o])
  (:import [org.apache.kafka.clients.admin AdminClient OffsetSpec ListOffsetsResult$ListOffsetsResultInfo]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer]))

(def ds o/ds)
(def opts {:builder-fn rs/as-unqualified-lower-maps})

;; =========================================================
;; 注文サービスの DB（orders と outbox）
;; =========================================================

(defn reset-orders-db! []
  (jdbc/execute! ds ["DROP TABLE IF EXISTS outbox, orders"])
  (jdbc/execute! ds ["CREATE TABLE orders (order_id TEXT PRIMARY KEY, product_id TEXT NOT NULL,
                                          qty INTEGER NOT NULL, amount INTEGER NOT NULL)"])
  (jdbc/execute! ds ["CREATE TABLE outbox (id BIGSERIAL PRIMARY KEY, order_id TEXT NOT NULL,
                                          payload TEXT NOT NULL, sent_at TIMESTAMPTZ)"])
  :ok)

(defn- new-order [i]
  {:order-id (o/order-id i) :product-id (str "p-" (rand-int o/n-products))
   :qty 1 :amount (+ 500 (rand-int 5000))})

(defn- payload [{:keys [order-id product-id qty amount]}]
  (str/join "|" [order-id product-id qty amount]))

(defn- insert-order! [tx {:keys [order-id product-id qty amount]}]
  (jdbc/execute-one! tx ["INSERT INTO orders VALUES (?, ?, ?, ?)" order-id product-id qty amount]))

(defn- make-producer []
  (KafkaProducer. ^java.util.Map {"bootstrap.servers" o/bootstrap "acks" "all"}
                  (StringSerializer.) (StringSerializer.)))

;; =========================================================
;; 4a：二重書き込み
;; =========================================================

(defn place-orders-dual!
  "n 件の注文を、(A) orders に保存 → (B) Kafka に送信、の順に受け付ける。
   crash-at に含まれる番号の注文では、(A) のあと (B) の前に落ちる。"
  [topic n crash-at]
  (with-open [p (make-producer)]
    (let [crashed (atom [])]
      (dotimes [i n]
        (let [order (new-order i)]
          (try
            (insert-order! ds order)                                                   ; (A)
            (when (contains? crash-at i)
              (throw (ex-info "crash between (A) and (B)" {:order-id (:order-id order)})))
            @(.send p (ProducerRecord. topic (:order-id order) (payload order)))      ; (B)
            (catch clojure.lang.ExceptionInfo e
              (swap! crashed conj (:order-id (ex-data e)))))))
      {:crashed @crashed})))

;; =========================================================
;; 4b：outbox
;; =========================================================

(defn place-orders-outbox!
  "n 件の注文を、orders と outbox への書き込みを1つのトランザクションにして受け付ける。
   crash-at に含まれる番号の注文では、orders に書いたあと、outbox に書く前に落ちる。"
  [n crash-at]
  (let [crashed (atom [])]
    (dotimes [i n]
      (let [order (new-order i)]
        (try
          (jdbc/with-transaction [tx ds]
            (insert-order! tx order)
            (when (contains? crash-at i)
              (throw (ex-info "crash inside transaction" {:order-id (:order-id order)})))
            (jdbc/execute-one! tx ["INSERT INTO outbox (order_id, payload) VALUES (?, ?)"
                                   (:order-id order) (payload order)]))
          (catch clojure.lang.ExceptionInfo e
            (swap! crashed conj (:order-id (ex-data e)))))))
    {:crashed @crashed}))

(defn- relay-loop
  "outbox の未送信の行を、100件ずつ Kafka に送り、送信済みの印を付ける。
   crash? が true なら、送ったあと印を付ける前に終了する。"
  [topic stop? crash?]
  (try
    (with-open [p (make-producer)]
      (loop []
        (when-not @stop?
          (let [rows (jdbc/execute! ds ["SELECT id, order_id, payload FROM outbox
                                         WHERE sent_at IS NULL ORDER BY id LIMIT 100"] opts)]
            (if (empty? rows)
              (do (Thread/sleep 200) (recur))
              (do
                ;; ① Kafka に送り、全件の ack を待つ
                (doseq [f (mapv (fn [{:keys [order_id payload]}]
                                  (.send p (ProducerRecord. topic order_id payload)))
                                rows)]
                  @f)
                (if @crash?
                  (println (str "[relay] crash!（" (count rows) " 件を送ったが、送信済みの印を付ける前に終了）"))
                  (do
                    ;; ② 送信済みの印を付ける
                    (jdbc/execute! ds (into [(str "UPDATE outbox SET sent_at = now() WHERE id IN ("
                                                  (str/join "," (repeat (count rows) "?")) ")")]
                                            (map :id rows)))
                    (recur)))))))))
    (catch Exception e
      (println "[relay] error:" e))))

(defonce relay (atom nil))

(defn start-relay!
  "リレーを起動する。crash-after-first-send? が true なら、最初の100件を送ったあと、印を付ける前に落ちる。"
  [topic & {:keys [crash-after-first-send?]}]
  (let [stop?  (atom false)
        crash? (atom (boolean crash-after-first-send?))
        fut    (future (relay-loop topic stop? crash?))]
    (reset! relay {:stop? stop? :fut fut})
    :started))

(defn stop-relay! []
  (when-let [{:keys [stop? fut]} @relay]
    (reset! stop? true)
    @fut
    (reset! relay nil)
    :stopped))

;; =========================================================
;; 確認用
;; =========================================================

(defn- topic-count
  "トピックに入っているメッセージの数（各パーティションの末尾 offset の合計）。"
  [topic]
  (with-open [a (AdminClient/create ^java.util.Map {"bootstrap.servers" o/bootstrap})]
    (let [tps  (mapv #(TopicPartition. topic %) (range o/n-partitions))
          ends (-> (.listOffsets a (into {} (for [tp tps] [tp (OffsetSpec/latest)]))) (.all) (.get))]
      (reduce + (for [tp tps]
                  (.offset ^ListOffsetsResult$ListOffsetsResultInfo (get ends tp)))))))

(defn counts [topic]
  {:orders-in-db    (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM orders"] opts))
   :outbox-unsent   (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM outbox WHERE sent_at IS NULL"] opts))
   :events-in-kafka (topic-count topic)})
