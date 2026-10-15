(ns kafka-study.day4.orders
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc])
  (:import [java.time Duration]
           [java.util.concurrent.atomic AtomicLong]
           [org.apache.kafka.clients.admin AdminClient OffsetSpec ListOffsetsResult$ListOffsetsResultInfo]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord OffsetAndMetadata ConsumerRebalanceListener]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.serialization StringSerializer StringDeserializer]))

(def bootstrap "localhost:9092,localhost:9094,localhost:9096")
(def n-partitions 3)
(def n-products 10)
(def initial-stock 100000)

;; =========================================================
;; DB（在庫と決済）
;; =========================================================

(def ds (jdbc/get-datasource {:dbtype "postgresql" :host "localhost" :port 5438
                              :dbname "shop" :user "kafka" :password "kafka"}))

;; (defn reset-db!
;;   "在庫と決済のテーブルを作り直す。在庫は各商品 initial-stock 個から始める。
;;    payments には、まだ一意制約を付けない。"
;;   []
;;   (jdbc/execute! ds ["DROP TABLE IF EXISTS payments, stock"])
;;   (jdbc/execute! ds ["CREATE TABLE stock (product_id TEXT PRIMARY KEY, quantity INTEGER NOT NULL)"])
;;   (jdbc/execute! ds ["CREATE TABLE payments (id SERIAL PRIMARY KEY, order_id TEXT NOT NULL, amount INTEGER NOT NULL)"])
;;   (doseq [i (range n-products)]
;;     (jdbc/execute! ds ["INSERT INTO stock VALUES (?, ?)" (str "p-" i) initial-stock]))
;;   :ok)

(defonce skipped (AtomicLong.))   ; 2回目以降に届いたので何もしなかった回数

(defn reset-db!
  "在庫と決済のテーブルを作り直す。在庫は各商品 initial-stock 個から始める。
   unique? が true なら、payments.order_id に一意制約を付ける。"
  [& {:keys [unique?]}]
  (.set ^AtomicLong skipped 0)
  (jdbc/execute! ds ["DROP TABLE IF EXISTS payments, stock"])
  (jdbc/execute! ds ["CREATE TABLE stock (product_id TEXT PRIMARY KEY, quantity INTEGER NOT NULL)"])
  (jdbc/execute! ds [(str "CREATE TABLE payments (id SERIAL PRIMARY KEY, order_id TEXT NOT NULL"
                          (when unique? " UNIQUE")
                          ", amount INTEGER NOT NULL)")])
  (doseq [i (range n-products)]
    (jdbc/execute! ds ["INSERT INTO stock VALUES (?, ?)" (str "p-" i) initial-stock]))
  :ok)

(defn audit
  "送った注文 sent 件に対して、決済と在庫が正しいかを調べる（どの注文も数量は1）。"
  [sent]
  (let [{:keys [payments paid]} (jdbc/execute-one! ds ["SELECT count(*) AS payments, count(DISTINCT order_id) AS paid FROM payments"]
                                                   {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})
        {:keys [consumed]}      (jdbc/execute-one! ds ["SELECT sum(? - quantity) AS consumed FROM stock" initial-stock]
                                                   {:builder-fn next.jdbc.result-set/as-unqualified-lower-maps})]
    {:sent               sent
     :paid-orders        paid
     :lost-orders        (- sent paid)
     :payment-rows       payments
     :duplicate-payments (- payments paid)
     :stock-consumed     consumed
     :skipped-duplicates (.get ^AtomicLong skipped)}))

;; =========================================================
;; 注文サービス側：注文イベントを送る
;; =========================================================

(defn order-id [i] (format "o-%06d" i))

(defn send-orders!
  "n 件の注文イベントを送る。値は 注文ID|商品ID|数量|金額（数量は常に1）。"
  [topic n]
  (with-open [p (KafkaProducer. ^java.util.Map {"bootstrap.servers" bootstrap "acks" "all"}
                                (StringSerializer.) (StringSerializer.))]
    (dotimes [i n]
      (let [oid (order-id i)]
        (.send p (ProducerRecord. topic oid
                                  (str/join "|" [oid (str "p-" (rand-int n-products)) 1 (+ 500 (rand-int 5000))])))))
    (.flush p)
    n))

(defn parse-order [v]
  (let [[oid pid qty amount] (str/split v #"\|")]
    {:order-id oid :product-id pid :qty (parse-long qty) :amount (parse-long amount)}))

;; =========================================================
;; 在庫・決済サービス側：注文を処理する consumer
;; =========================================================

(defn- process-naive!
  "在庫を減らし、決済を記録する（1つのトランザクション）。重複の対策はしていない。"
  [{:keys [order-id product-id qty amount]}]
  (jdbc/with-transaction [tx ds]
    (jdbc/execute-one! tx ["INSERT INTO payments (order_id, amount) VALUES (?, ?)" order-id amount])
    (jdbc/execute-one! tx ["UPDATE stock SET quantity = quantity - ? WHERE product_id = ?" qty product-id])))

(defn- process-idempotent!
  "決済を記録し、記録できたときだけ在庫を減らす（1つのトランザクション）。
   同じ注文IDがすでに記録されていれば、何もしない。"
  [{:keys [order-id product-id qty amount]}]
  (jdbc/with-transaction [tx ds]
    (let [r (jdbc/execute-one! tx ["INSERT INTO payments (order_id, amount) VALUES (?, ?)
                                    ON CONFLICT (order_id) DO NOTHING"
                                   order-id amount])]
      (if (= 1 (:next.jdbc/update-count r))
        (jdbc/execute-one! tx ["UPDATE stock SET quantity = quantity - ? WHERE product_id = ?" qty product-id])
        (.incrementAndGet ^AtomicLong skipped)))))

(defn- make-consumer [group]
  (KafkaConsumer. ^java.util.Map {"bootstrap.servers"  bootstrap
                                  "group.id"           group
                                  "auto.offset.reset"  "earliest"
                                  "enable.auto.commit" "false"
                                  "max.poll.records"   "100"}
                  (StringDeserializer.)
                  (StringDeserializer.)))

(defn- listener [id]
  (reify ConsumerRebalanceListener
    (onPartitionsRevoked [_ parts]
      (println (str "[" id "] revoked  " (vec (sort (map #(.partition ^TopicPartition %) parts))))))
    (onPartitionsAssigned [_ parts]
      (println (str "[" id "] assigned " (vec (sort (map #(.partition ^TopicPartition %) parts))))))))

(defn- worker-loop
  "mode = :commit-first … poll の直後に commit してから処理する
          :commit-after … 処理してから commit する
          :idempotent   … 処理してから commit する。処理は冪等（2回目は何もしない）"
  [id topic group mode proc-ms stop? crash?]
  (let [process!      (if (= mode :idempotent) process-idempotent! process-naive!)
        commit-after? (contains? #{:commit-after :idempotent} mode)]
    (try
      (with-open [c (make-consumer group)]
        (.subscribe c [topic] (listener id))
        (loop []
          (when-not @stop?
            (let [recs (vec (.poll c (Duration/ofMillis 200)))]
              (when (and (seq recs) (= mode :commit-first))
                (.commitSync c))
              (let [done (reduce (fn [k ^ConsumerRecord r]
                                   (if @crash?
                                     (reduced k)
                                     (do (Thread/sleep (long proc-ms))   ; 決済サービスの呼び出しなどの代わり
                                         (process! (parse-order (.value r)))
                                         (inc k))))
                                 0 recs)]
                (if @crash?
                  (println (str "[" id "] crash!（" (count recs) " 件中 " done " 件を処理したところで終了）"))
                  (do (when (and (seq recs) commit-after?)
                        (.commitSync c))
                      (recur))))))))
      (catch Exception e
        (println (str "[" id "] error: " e))))))

(defonce workers (atom {}))

(defn start-worker!
  [id mode & {:keys [topic group proc-ms] :or {group "orders" proc-ms 20}}]
  (let [stop?  (atom false)
        crash? (atom false)
        fut    (future (worker-loop id topic group mode proc-ms stop? crash?))]
    (swap! workers assoc id {:stop? stop? :crash? crash? :fut fut})
    id))

(defn crash-worker!
  "処理の途中で、その consumer を終了させる（commit はしない）。"
  [id]
  (when-let [{:keys [crash? fut]} (get @workers id)]
    (reset! crash? true)
    @fut
    (swap! workers dissoc id)
    id))

(defn stop-all! []
  (doseq [[id {:keys [stop? fut]}] @workers]
    (reset! stop? true)
    @fut)
  (reset! workers {}))

;; =========================================================
;; lag（処理が終わったかの確認）
;; =========================================================

(defn lag
  "group の lag の合計。"
  [topic group]
  (with-open [a (AdminClient/create ^java.util.Map {"bootstrap.servers" bootstrap})]
    (let [tps       (mapv #(TopicPartition. topic %) (range n-partitions))
          committed (-> (.listConsumerGroupOffsets a group) (.partitionsToOffsetAndMetadata) (.get))
          ends      (-> (.listOffsets a (into {} (for [tp tps] [tp (OffsetSpec/latest)]))) (.all) (.get))]
      (reduce + (for [^TopicPartition tp tps]
                  (- (.offset ^ListOffsetsResult$ListOffsetsResultInfo (get ends tp))
                     (if-let [^OffsetAndMetadata om (get committed tp)] (.offset om) 0)))))))

(defn wait-done!
  "lag が 0 になるまで待つ。"
  [topic group]
  (loop []
    (let [l (lag topic group)]
      (when (pos? l)
        (println "lag =" l)
        (Thread/sleep 2000)
        (recur))))
  (println "lag = 0（全件 commit 済み）"))
