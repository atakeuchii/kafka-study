(ns kafka-study.day1.app
  (:require [clojure.string :as str]))

(def apps ["web" "api" "batch"])

;; 通し番号。あとで「何件消えたか」を数えるために使う
(def seq-no (atom 0))

(defn make-line
  "ログ1行を作る。形式は 時刻ms|アプリ名|レベル|msg-通し番号。
   約10%が ERROR。"
  []
  (let [n     (swap! seq-no inc)
        level (if (< (rand) 0.1) "ERROR" "INFO")]
    (str/join "|" [(System/currentTimeMillis) (rand-nth apps) level (str "msg-" n)])))
