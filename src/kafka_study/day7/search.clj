(ns kafka-study.day7.search
  "Day 7：検索インデックス同期（compacted topic・tombstone・再構築）"
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (java.time Duration)
           (java.util Random)
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

(def ds (jdbc/get-datasource {:dbtype "postgresql" :host "localhost" :port 5438
                              :dbname "shop" :user "kafka" :password "kafka"}))

;; compacted topic の設定。確認しやすいように、セグメントを短く・掃除の条件をゆるくしている
(defn compact-configs [delete-retention-ms]
  {"cleanup.policy"            "compact"
   "segment.ms"                "5000"     ; 5 秒たったら、次の書き込みでセグメントを切り替える
   "min.cleanable.dirty.ratio" "0.01"     ; 少しでも古い値があれば掃除の対象にする
   "delete.retention.ms"       (str delete-retention-ms)}) ; tombstone を残す時間

;; ============================================================
;; 商品データ
;; ============================================================

(def adjectives ["薄型" "軽量" "大容量" "静音" "防水" "高速" "ワイヤレス" "コンパクト"])
(def nouns      ["ノートPC" "キーボード" "マウス" "モニター" "イヤホン" "スピーカー" "カメラ" "SSD"])

(defn pid [i] (format "p%04d" i))

(defn- rand-name [^Random rnd]
  (str (nth adjectives (.nextInt rnd (count adjectives))) " "
       (nth nouns (.nextInt rnd (count nouns)))))

(defn- rand-price [^Random rnd] (* 100 (+ 10 (.nextInt rnd 200))))

;; ============================================================
;; PostgreSQL（正しいデータ）
;; ============================================================

(def ^:private opts {:builder-fn rs/as-unqualified-maps})

(defn reset-db! []
  (jdbc/execute! ds ["DROP TABLE IF EXISTS products"])
  (jdbc/execute! ds ["CREATE TABLE products (
                        id      TEXT PRIMARY KEY,
                        name    TEXT NOT NULL,
                        price   INT  NOT NULL,
                        version INT  NOT NULL)"])
  (println "products テーブルを作り直しました"))

(defn db-upsert!
  "なければ登録、あれば更新。version は DB が 1 ずつ増やす（同じ行の更新は DB が 1 つずつ順番に行う）。
   戻り値：更新後の行 {:id :name :price :version}"
  [id name price]
  (jdbc/execute-one! ds ["INSERT INTO products (id, name, price, version) VALUES (?, ?, ?, 1)
                          ON CONFLICT (id) DO UPDATE
                            SET name = EXCLUDED.name, price = EXCLUDED.price,
                                version = products.version + 1
                          RETURNING id, name, price, version" id name price]
                     opts))

(defn db-delete! [id]
  (jdbc/execute-one! ds ["DELETE FROM products WHERE id = ?" id]))

(defn db-snapshot
  "DB の全商品：id → {:name :price :version}"
  []
  (into {} (for [r (jdbc/execute! ds ["SELECT id, name, price, version FROM products"] opts)]
             [(:id r) (dissoc r :id)])))

;; ============================================================
;; 検索インデックス（メモリ上の簡単な検索エンジン）
;; ============================================================

(defonce indexes (atom {}))          ; インデックス名 → atom {:docs {id doc} :terms {単語 #{id}}}
(defonce live (atom nil))            ; お客さんの検索が使うインデックス名（エイリアス）
(defonce faults (atom {:fail-rate 0.0 :max-delay-ms 0}))  ; 二重書き込み版の「検索エンジンの調子」

(defn- tokens [name] (set (str/split name #"\s+")))

(defn- idx-delete [idx id]
  (if-let [old (get-in idx [:docs id])]
    (-> (reduce (fn [i t] (update-in i [:terms t] disj id)) idx (tokens (:name old)))
        (update :docs dissoc id))
    idx))

(defn- idx-put [idx {:keys [id name] :as doc}]
  (-> (idx-delete idx id)
      (as-> i (reduce (fn [i t] (update-in i [:terms t] (fnil conj #{}) id)) i (tokens name)))
      (assoc-in [:docs id] (dissoc doc :id))))

(defn create-index! [iname]
  (swap! indexes assoc iname (atom {:docs {} :terms {}}))
  (println "インデックス作成:" iname)
  iname)

(defn index-put! [iname doc] (swap! (get @indexes iname) idx-put doc))
(defn index-delete! [iname id] (swap! (get @indexes iname) idx-delete id))
(defn index-docs [iname] (:docs @(get @indexes iname)))

(defn switch-live!
  "お客さんの検索が使うインデックスを切り替える"
  [iname]
  (println (format "検索の向き先: %s → %s" @live iname))
  (reset! live iname))

(defn search
  "live のインデックスで単語検索する。ヒット件数と先頭 n 件を返す"
  [word & {:keys [n] :or {n 5}}]
  (let [idx  @(get @indexes @live)
        ids  (sort (get-in idx [:terms word]))]
    {:index @live
     :hits  (count ids)
     :top   (for [id (take n ids)] (assoc (get-in idx [:docs id]) :id id))}))

(defn remote-index-put!
  "二重書き込み版：検索エンジンに『ネットワーク越しに』書く。faults に従って遅れたり失敗したりする"
  [iname doc]
  (let [{:keys [fail-rate max-delay-ms]} @faults]
    (when (pos? max-delay-ms) (Thread/sleep (long (rand-int (inc max-delay-ms)))))
    (when (< (rand) fail-rate)
      (throw (ex-info "検索エンジンに書けませんでした" {:id (:id doc)})))
    (index-put! iname doc)))

(defn remote-index-delete! [iname id]
  (let [{:keys [fail-rate]} @faults]
    (when (< (rand) fail-rate)
      (throw (ex-info "検索エンジンから消せませんでした" {:id id})))
    (index-delete! iname id)))

(defn compare-with-db
  "DB とインデックスを突き合わせる（名前と値段で比較）
   :mismatch 両方にあるが値が違う / :missing DB にあるのに検索にない / :ghost DB にないのに検索にある"
  [iname]
  (let [db   (db-snapshot)
        ix   (index-docs iname)
        diff (for [id (sort (into (set (keys db)) (keys ix)))
                   :let [d (some-> (get db id) (select-keys [:name :price]))
                         x (some-> (get ix id) (select-keys [:name :price]))]
                   :when (not= d x)]
               {:id id :db d :index x})]
    {:index    iname
     :db       (count db)
     :docs     (count ix)
     :mismatch (count (filter #(and (:db %) (:index %)) diff))
     :missing  (count (filter #(and (:db %) (nil? (:index %))) diff))
     :ghost    (count (filter #(and (nil? (:db %)) (:index %)) diff))
     :examples (vec (take 3 diff))}))

;; ============================================================
;; Step 1：二重書き込み
;; ============================================================

(defn dual-update! [iname id name price]
  (let [row (db-upsert! id name price)]   ; (A) DB
    (remote-index-put! iname row)         ; (B) 検索エンジン
    row))

(defn dual-delete! [iname id]
  (db-delete! id)                         ; (A) DB
  (remote-index-delete! iname id))        ; (B) 検索エンジン

(defn run-dual-failures!
  "1a：検索エンジンへの書き込みが fail-rate の確率で失敗する。
   products 個の商品を rounds 回ずつ更新し、最後に deletes 個を削除する"
  [& {:keys [products rounds deletes fail-rate] :or {products 100 rounds 10 deletes 20 fail-rate 0.05}}]
  (reset-db!)
  (let [iname (create-index! "dual-a")
        rnd   (Random. 7)
        try!  (fn [f] (try (f) :ok (catch clojure.lang.ExceptionInfo _ :failed)))]
    (reset! live iname)
    (reset! faults {:fail-rate fail-rate :max-delay-ms 0})
    (let [ups (frequencies (for [_ (range rounds) i (range products)]
                             (try! #(dual-update! iname (pid i) (rand-name rnd) (rand-price rnd)))))
          dels (frequencies (for [i (range deletes)]
                              (try! #(dual-delete! iname (pid i)))))]
      (reset! faults {:fail-rate 0.0 :max-delay-ms 0})
      (println "アプリから見た結果  更新:" ups " 削除:" dels)
      (compare-with-db iname))))

(defn run-dual-race!
  "1b：サーバー X と Y が同じ商品を同時に更新する。失敗はないが、(A) と (B) の間に 0〜max-delay-ms 待つ"
  [& {:keys [products rounds max-delay-ms] :or {products 20 rounds 30 max-delay-ms 3}}]
  (reset-db!)
  (let [iname (create-index! "dual-b")
        rnd   (Random. 8)
        names (vec (for [_ (range products)] (rand-name rnd)))]
    (reset! live iname)
    (reset! faults {:fail-rate 0.0 :max-delay-ms max-delay-ms})
    ;; 同じ商品を X（1 万円台）と Y（2 万円台）が同時に更新する。終わったら次の商品へ
    (doseq [r (range rounds) i (range products)]
      (let [x (future (dual-update! iname (pid i) (names i) (+ 10000 r)))
            y (future (dual-update! iname (pid i) (names i) (+ 20000 r)))]
        @x @y))
    (reset! faults {:fail-rate 0.0 :max-delay-ms 0})
    (compare-with-db iname)))

;; ============================================================
;; Kafka 共通
;; ============================================================

(defn- admin ^AdminClient [] (AdminClient/create {"bootstrap.servers" bootstrap}))

(defn create-topic! [topic partitions configs]
  (with-open [a (admin)]
    (-> (.createTopics a [(doto (NewTopic. ^String topic (int partitions) (short replication))
                            (.configs ^java.util.Map configs))])
        .all .get))
  (println "トピック作成:" topic partitions "パーティション" (if (seq configs) configs ""))
  topic)

(defn- run-id [label] (str label "-" (mod (System/currentTimeMillis) 100000)))

(defn producer-props []
  {"bootstrap.servers" bootstrap
   "key.serializer"    (.getName StringSerializer)
   "value.serializer"  (.getName StringSerializer)
   "acks"              "all"
   "linger.ms"         "5"})

(defn make-producer ^KafkaProducer [] (KafkaProducer. ^java.util.Map (producer-props)))

(defn- consumer-props [group]
  {"bootstrap.servers"  bootstrap
   "group.id"           group
   "key.deserializer"   (.getName StringDeserializer)
   "value.deserializer" (.getName StringDeserializer)
   "enable.auto.commit" "false"
   "auto.offset.reset"  "earliest"
   "max.poll.records"   "500"})

(defn- plain-consumer-props []
  {"bootstrap.servers"  bootstrap
   "key.deserializer"   (.getName StringDeserializer)
   "value.deserializer" (.getName StringDeserializer)
   "enable.auto.commit" "false"})

;; ============================================================
;; アプリ（DB を更新してから Kafka に送る）
;; ============================================================

(defn app-update!
  "DB を更新し、更新後の行を送る。key = 商品ID（keyless? なら key なし）。
   delay-ms を渡すと、DB の更新と送信の間に 0〜delay-ms 待つ（Q2-3 の X の待ち）"
  [^KafkaProducer prod topic id name price & {:keys [keyless? delay-ms]}]
  (let [row (db-upsert! id name price)]
    (when delay-ms (Thread/sleep (long (rand-int (inc delay-ms)))))
    (.send prod (ProducerRecord. ^String topic (when-not keyless? id) (pr-str row)))
    row))

(defn app-delete!
  "DB から消し、tombstone（key = 商品ID、value = null）を送る"
  [^KafkaProducer prod topic id]
  (db-delete! id)
  (.send prod (ProducerRecord. ^String topic id nil)))

;; ============================================================
;; indexer（consumer 1 台 = スレッド 1 本）
;; ============================================================

(defonce stats (atom {}))   ; インデックス名 → {:applied :deleted :stale-skipped}
(defn- bump! [iname k] (swap! stats update-in [iname k] (fnil inc 0)))

(defn- apply-event!
  "1 件をインデックスに反映する。
   value = null（tombstone）→ 削除
   versioned? なら、手元より古い version の更新は無視する"
  [iname versioned? r]
  (let [k (.key r) v (.value r)]
    (if (nil? v)
      (do (index-delete! iname k) (bump! iname :deleted))
      (let [doc (edn/read-string v)
            cur (get (index-docs iname) (:id doc))]
        (if (and versioned? cur (<= (:version doc) (:version cur)))
          (bump! iname :stale-skipped)
          (do (index-put! iname doc) (bump! iname :applied)))))))

(defonce indexers (atom {}))

(defn- indexer-loop [wname {:keys [topic group index versioned? slow]} running assigned]
  (let [c (KafkaConsumer. ^java.util.Map (consumer-props group))]
    (try
      (.subscribe c [topic]
                  (reify ConsumerRebalanceListener
                    (onPartitionsRevoked [_ ps]
                      (swap! assigned #(reduce disj % (map (fn [^TopicPartition tp] (.partition tp)) ps))))
                    (onPartitionsAssigned [_ ps]
                      (swap! assigned into (map (fn [^TopicPartition tp] (.partition tp)) ps)))))
      (while @running
        (let [recs (.poll c (Duration/ofMillis 200))]
          (doseq [r recs]
            ;; slow = {:partition 1 :ms 2} なら、そのパーティションだけ 1 件ごとに待つ（詰まっている indexer）
            (when (and slow (= (:partition slow) (.partition r)))
              (Thread/sleep (long (:ms slow))))
            (apply-event! index versioned? r))
          (when-not (.isEmpty recs) (.commitSync c))))
      (catch Exception e (println (format "[%s] 例外: %s" wname e)))
      (finally (.close c)))))

(defn start-indexers!
  "opts = {:topic :group :index :versioned? :slow}"
  [names opts]
  (doseq [n names]
    (let [running (atom true) assigned (atom #{})
          t (doto (Thread. ^Runnable #(indexer-loop n opts running assigned)) (.setName n) (.start))]
      (swap! indexers assoc n {:running running :thread t :assigned assigned :group (:group opts)}))))

(defn stop-indexers!
  "group を指定するとその group の indexer だけ、指定しなければ全部止める"
  ([] (stop-indexers! nil))
  ([group]
   (let [ws (filter (fn [[_ w]] (or (nil? group) (= group (:group w)))) @indexers)]
     (doseq [[_ w] ws] (reset! (:running w) false))
     (doseq [[n w] ws] (.join ^Thread (:thread w)) (swap! indexers dissoc n)))))

(defn wait-assigned!
  "group の indexer に partitions 個のパーティションが割り当て終わるまで待つ"
  [group partitions]
  (loop [i 0]
    (let [ws (filter #(= group (:group (val %))) @indexers)
          n  (count (mapcat (comp deref :assigned val) ws))]
      (cond (= n partitions) (println "割り当て完了:" group)
            (> i 300)        (println "割り当て待ちタイムアウト")
            :else            (do (Thread/sleep 200) (recur (inc i)))))))

(defn group-lag [group topic]
  (with-open [a (admin)
              c (KafkaConsumer. ^java.util.Map (plain-consumer-props))]
    (let [tps       (mapv #(TopicPartition. ^String topic (int (.partition %))) (.partitionsFor c topic))
          ends      (.endOffsets c tps)
          committed (-> a (.listConsumerGroupOffsets ^String group) .partitionsToOffsetAndMetadata .get)]
      (reduce + (for [tp tps]
                  (- (get ends tp) (if-let [om (get committed tp)] (.offset om) 0)))))))

(defn wait-done!
  "group の lag が 0 になるまで待つ。かかった秒数を返す"
  [group topic & {:keys [timeout-s] :or {timeout-s 180}}]
  (let [start (System/currentTimeMillis)]
    (loop []
      (let [l       (group-lag group topic)
            elapsed (/ (- (System/currentTimeMillis) start) 1000.0)]
        (cond
          (zero? l)             (do (println (format "処理完了 %s（lag 0、%.1f 秒）" group elapsed)) elapsed)
          (> elapsed timeout-s) (do (println "タイムアウト。lag =" l) elapsed)
          :else                 (do (Thread/sleep 300) (recur)))))))

;; ============================================================
;; Step 2：Kafka 経由の同期
;; ============================================================

(defn run-sync!
  "products 個の商品を rounds 周更新する（1 周ごとに全商品を 1 回ずつ。同じ商品の更新は products 件離れる）。
   keyless? でキーなし。
   indexer が止まっていた間に変更が溜まった、という想定で、全部送ってから indexer 3 台を起動する。
   パーティション 1 の担当だけ 1 件 slow-ms 待つ（詰まっている indexer）"
  [label & {:keys [keyless? products rounds slow-ms] :or {products 1000 rounds 10 slow-ms 1}}]
  (stop-indexers!)
  (reset-db!)
  (let [id    (run-id label)
        topic (create-topic! (str "products-" id) 3 {})
        group (str "indexer-" id)
        iname (create-index! label)
        rnd   (Random. 11)]
    (reset! live iname)
    (swap! stats dissoc iname)
    (with-open [p (make-producer)]
      (doseq [_ (range rounds) i (range products)]
        (app-update! p topic (pid i) (rand-name rnd) (rand-price rnd) :keyless? keyless?))
      (.flush p))
    (println (format "送信: %d 件（%s）" (* products rounds) (if keyless? "キーなし" "キー = 商品ID")))
    (println "indexer 起動（溜まった変更をまとめて反映する）")
    (start-indexers! [(str label "-1") (str label "-2") (str label "-3")]
                     {:topic topic :group group :index iname :slow {:partition 1 :ms slow-ms}})
    (wait-done! group topic)
    (stop-indexers! group)
    (assoc (compare-with-db iname) :stats (get @stats iname))))

(defn run-sync-race!
  "Q2-3：サーバー X と Y が、キー = 商品ID で、DB 更新の 0〜delay-ms 後に送る。
   versioned? なら indexer は古い version を無視する"
  [label & {:keys [versioned? products rounds delay-ms] :or {products 20 rounds 30 delay-ms 3}}]
  (stop-indexers!)
  (reset-db!)
  (let [id    (run-id label)
        topic (create-topic! (str "products-" id) 3 {})
        group (str "indexer-" id)
        iname (create-index! label)
        rnd   (Random. 12)
        names (vec (for [_ (range products)] (rand-name rnd)))]
    (reset! live iname)
    (swap! stats dissoc iname)
    (start-indexers! [(str label "-1") (str label "-2") (str label "-3")]
                     {:topic topic :group group :index iname :versioned? versioned?})
    (wait-assigned! group 3)
    (with-open [p (make-producer)]
      ;; 同じ商品を X（1 万円台）と Y（2 万円台）が同時に更新する。終わったら次の商品へ
      (doseq [r (range rounds) i (range products)]
        (let [x (future (app-update! p topic (pid i) (names i) (+ 10000 r) :delay-ms delay-ms))
              y (future (app-update! p topic (pid i) (names i) (+ 20000 r) :delay-ms delay-ms))]
          @x @y))
      (.flush p))
    (wait-done! group topic)
    (stop-indexers! group)
    (assoc (compare-with-db iname) :stats (get @stats iname))))

;; ============================================================
;; Step 3：compaction を観察する
;; ============================================================

(defn dump-topic
  "トピックの partition p を先頭から末尾まで読み、offset・key・value を表示する（_tick は件数にだけ含める）。
   戻り値 {:records 件数 :first-offset 先頭の offset :end-offset 次に書かれる offset}"
  [topic & {:keys [p print?] :or {p 0 print? true}}]
  (with-open [c (KafkaConsumer. ^java.util.Map (plain-consumer-props))]
    (let [tp  (TopicPartition. ^String topic (int p))
          tps [tp]]
      (.assign c tps)
      (.seekToBeginning c tps)
      (let [begin (.position c tp)
            end   (get (.endOffsets c tps) tp)]
        (loop [n 0]
          (if (>= (.position c tp) end)
            (do (println (format "--- %d 件（先頭 offset %d、末尾 offset %d）" n begin end))
                {:records n :first-offset begin :end-offset end})
            (let [recs (.poll c (Duration/ofMillis 200))]
              (when print?
                (doseq [r recs :when (not= "_tick" (.key r))]   ; tick（Step 3 の時計代わり）は表示しない
                  (println (format "  offset %3d  key %-6s value %s"
                                   (.offset r) (.key r) (or (.value r) "null（tombstone）")))))
              (recur (+ n (.count recs))))))))))

(defn create-demo-topic!
  "Step 3 用：1 パーティションの compacted topic"
  [& {:keys [delete-retention-ms] :or {delete-retention-ms 20000}}]
  (create-topic! (str "products-demo-" (mod (System/currentTimeMillis) 100000)) 1
                 (compact-configs delete-retention-ms)))

(defn send-demo-events!
  "p1 を 10 回更新、p2 を 3 回更新、p3 を登録して削除、p4 を 1 回登録"
  [topic]
  (with-open [p (make-producer)]
    (let [send! (fn [k v] (.send p (ProducerRecord. ^String topic k v)))]
      (doseq [[i price] (map-indexed vector (range 1000 0 -100))]
        (send! "p1" (pr-str {:id "p1" :name "薄型 ノートPC" :price price :version (inc i)})))
      (doseq [[i price] (map-indexed vector [500 480 450])]
        (send! "p2" (pr-str {:id "p2" :name "静音 キーボード" :price price :version (inc i)})))
      (send! "p3" (pr-str {:id "p3" :name "防水 イヤホン" :price 300 :version 1}))
      (send! "p3" nil)
      (send! "p4" (pr-str {:id "p4" :name "大容量 SSD" :price 1200 :version 1}))
      (.flush p)))
  (println "送信: p1×10、p2×3、p3（登録→削除）、p4×1 = 16 件"))

(defn tick!
  "seconds 秒間、1 秒ごとに key = _tick のメッセージを送る。
   セグメントの切り替えは『segment.ms を過ぎた後の書き込み』で起きるので、時計を進める代わりに書き込みを続ける"
  [topic seconds]
  (with-open [p (make-producer)]
    (dotimes [i seconds]
      (.send p (ProducerRecord. ^String topic "_tick" (str "tick-" i)))
      (.flush p)
      (Thread/sleep 1000)))
  (println (format "tick を %d 秒送りました" seconds)))

;; ============================================================
;; Step 4：インデックスを捨てて作り直す
;; ============================================================

(defn topic-stats
  "トピック全体のレコード数・キーの種類・tombstone の数"
  [topic]
  (with-open [c (KafkaConsumer. ^java.util.Map (plain-consumer-props))]
    (let [tps (mapv #(TopicPartition. ^String topic (int (.partition %))) (.partitionsFor c topic))
          end (.endOffsets c tps)]
      (.assign c tps)
      (.seekToBeginning c tps)
      (loop [n 0 keys* #{} tomb 0]
        (if (every? #(>= (.position c %) (get end %)) tps)
          (let [r {:records n :keys (count keys*) :tombstones tomb
                   :end-offsets (reduce + (vals end))}]
            (println (format "%s: レコード %d 件 / キー %d 種類 / tombstone %d 件（offset の合計 %d）"
                             topic n (count keys*) tomb (:end-offsets r)))
            r)
          (let [recs (.poll c (Duration/ofMillis 200))]
            (recur (+ n (.count recs))
                   (into keys* (map #(.key %)) recs)
                   (+ tomb (count (filter #(nil? (.value %)) recs))))))))))

(defn setup-catalog!
  "Step 4 の準備：compacted topic を作り、商品 products 個を rounds 回ずつ更新し、deletes 個を削除する。
   indexer（group indexer-v1）がインデックス v1 に反映し、検索は v1 を向く。
   戻り値 ctx = {:topic :products}"
  [& {:keys [products rounds deletes] :or {products 2000 rounds 5 deletes 200}}]
  (stop-indexers!)
  (reset-db!)
  (let [topic (create-topic! (str "products-" (run-id "catalog")) 3 (compact-configs 60000))
        rnd   (Random. 13)]
    (create-index! "v1")
    (reset! live "v1")
    (swap! stats dissoc "v1")
    (start-indexers! ["v1-1" "v1-2" "v1-3"] {:topic topic :group "indexer-v1" :index "v1"})
    (wait-assigned! "indexer-v1" 3)
    (with-open [p (make-producer)]
      (doseq [_ (range rounds) i (range products)]
        (app-update! p topic (pid i) (rand-name rnd) (rand-price rnd)))
      (doseq [i (range deletes)]
        (app-delete! p topic (pid (* 10 i))))   ; p0000, p0010, p0020, … を削除
      (.flush p))
    (println (format "送信: 更新 %d 件 + 削除 %d 件" (* products rounds) deletes))
    (wait-done! "indexer-v1" topic)
    {:topic topic :products products}))

(defn corrupt-index!
  "バグのせいで、インデックスの n 件の値段が 0 円になったことにする"
  [iname n]
  (let [ids (take n (shuffle (keys (index-docs iname))))]
    (doseq [id ids]
      (index-put! iname (assoc (get (index-docs iname) id) :id id :price 0)))
    (println (format "%s の %d 件を 0 円にしました" iname (count ids)))))

(defonce writer (atom nil))

(defn start-writer!
  "お客さんが使っている間も商品は更新され続ける：1 秒に rate 件、ランダムな商品を更新する"
  [{:keys [topic products]} & {:keys [rate] :or {rate 20}}]
  (let [running (atom true)
        rnd     (Random. 14)
        f       (future
                  (with-open [p (make-producer)]
                    (while @running
                      (app-update! p topic (pid (.nextInt rnd (int products))) (rand-name rnd) (rand-price rnd))
                      (Thread/sleep (long (/ 1000 rate))))))]
    (reset! writer {:running running :future f})
    (println (format "writer 開始（1 秒に %d 件）" rate))))

(defn stop-writer! []
  (when-let [{:keys [running future]} @writer]
    (reset! running false)
    @future
    (reset! writer nil)
    (println "writer 停止")))

(defn rebuild!
  "新しいインデックス iname を作り、新しい group で topic を先頭から読ませる。lag 0 になるまで待つ。
   検索の向き先（live）は変えない"
  [{:keys [topic]} iname]
  (let [group (str "indexer-" iname "-" (mod (System/currentTimeMillis) 100000))]
    (create-index! iname)
    (swap! stats dissoc iname)
    (start-indexers! [(str iname "-1") (str iname "-2") (str iname "-3")]
                     {:topic topic :group group :index iname})
    (let [sec (wait-done! group topic)
          s   (get @stats iname)]
      (println (format "再構築 %s：%.1f 秒、読んだ件数 %d（反映 %d・削除 %d）、インデックス %d 件"
                       iname sec (+ (:applied s 0) (:deleted s 0)) (:applied s 0) (:deleted s 0)
                       (count (index-docs iname))))
      {:group group :seconds sec :stats s})))
