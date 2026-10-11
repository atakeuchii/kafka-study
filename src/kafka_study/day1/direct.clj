(ns kafka-study.day1.direct
  (:require [kafka-study.day1.app :as app]
            [kafka-study.day1.receivers :as r]))

(defn log!
  "Kafka なし版。3つの受け手を順番に直接呼ぶ。"
  [line]
  (r/write-file! line)
  (r/write-db! line)
  (r/check-alert! line))

(defn run-app!
  "アプリが n 回処理を行い、そのたびに log! を呼ぶ想定。
   log! が例外を投げたら、アプリの処理が失敗したとして数える。"
  [n]
  (let [start  (System/nanoTime)
        result (reduce
                (fn [acc _]
                  (let [line (app/make-line)
                        t0   (System/nanoTime)
                        ok?  (try (log! line) true
                                  (catch Exception _ false))
                        ms   (/ (- (System/nanoTime) t0) 1e6)]
                    (cond-> (update acc :max-log-ms max ms)
                      (not ok?) (update :app-failed inc))))
                {:max-log-ms 0.0 :app-failed 0}
                (range n))]
    (assoc result
           :n n
           :total-ms (/ (- (System/nanoTime) start) 1e6))))
