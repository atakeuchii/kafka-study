(ns kafka-study.day1.receivers
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def out-dir "out/day1")
(def out-files ["file-writer.log" "db-writer.log" "alerts.log"])

;; ---- 障害注入 ----
;; 受け手ごとに「遅延(ms)」と「停止中か」を設定できる

(def default-faults {:file  {:delay-ms 0 :down? false}
                     :db    {:delay-ms 0 :down? false}
                     :alert {:delay-ms 0 :down? false}})

(def faults (atom default-faults))

(defn- inject! [k]
  (let [{:keys [delay-ms down?]} (get @faults k)]
    (when down?
      (throw (ex-info (str (name k) " is down") {:receiver k})))
    (when (pos? delay-ms)
      (Thread/sleep (long delay-ms)))))

;; ---- 共通 ----

(defn- append! [file line]
  (let [f (io/file out-dir file)]
    (io/make-parents f)
    (spit f (str line "\n") :append true)))

(defn error? [line]
  (str/includes? line "|ERROR|"))

;; ---- ① ファイル保存：全行を保存する ----

(defn write-file! [line]
  (inject! :file)
  (append! "file-writer.log" line))

;; ---- ② DB 記録：ERROR だけを記録する（ファイルで代用） ----

(defn write-db! [line]
  (when (error? line)
    (inject! :db)
    (append! "db-writer.log" line)))

;; ---- ③ アラート：直近 window-ms 以内の ERROR が threshold 件に達したら発報 ----

(def alert-config {:window-ms 10000 :threshold 5})
(def alert-state (atom {:recent []}))

(defn check-alert! [line]
  (when (error? line)
    (inject! :alert)
    (let [now (System/currentTimeMillis)
          {:keys [window-ms threshold]} alert-config
          recent (-> (swap! alert-state update :recent
                            (fn [ts] (conj (filterv #(> % (- now window-ms)) ts) now)))
                     :recent)]
      (when (>= (count recent) threshold)
        (swap! alert-state assoc :recent [])
        (append! "alerts.log"
                 (str now " ALERT: " (count recent) " errors within " window-ms "ms"))))))

;; ---- 実験用ユーティリティ ----

(defn reset-all!
  "障害設定・アラートの状態・出力ファイルをすべて初期化する。"
  []
  (reset! faults default-faults)
  (reset! alert-state {:recent []})
  (doseq [f out-files]
    (io/delete-file (io/file out-dir f) true)))

(defn counts
  "各出力ファイルの行数を返す。"
  []
  (into (sorted-map)
        (for [f out-files
              :let [file (io/file out-dir f)]]
          [f (if (.exists file)
               (with-open [rdr (io/reader file)] (count (line-seq rdr)))
               0)])))
