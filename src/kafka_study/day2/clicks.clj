(ns kafka-study.day2.clicks)

(def actions ["view" "click" "add_to_cart"])

(defn make-event
  "行動ログ1件を [キー 値] で返す。キーはユーザーID（同じユーザーの行動は同じパーティションに入り、順序が保たれる）。
   値は約90バイトの JSON 風文字列。"
  []
  (let [user (str "u" (rand-int 100000))]
    [user
     (format "{\"user\":\"%s\",\"action\":\"%s\",\"page\":\"/items/%d\",\"ts\":%d}"
             user (rand-nth actions) (rand-int 10000) (System/currentTimeMillis))]))
