(ns kafka-study.day1.leak
  (:require [kafka-study.day1.kafka :as k])
  (:import [org.apache.kafka.clients.producer ProducerRecord]))

(def topic "close-test")

(defn -main
  "n 件を send してから、mode に応じて終了する。
   mode = none  : close も flush もせずに、すぐプロセスを終了する
   mode = close : close してから終了する"
  [n mode]
  (let [n     (parse-long n)
        p     (k/make-producer)
        start (System/nanoTime)]
    (dotimes [i n]
      (.send p (ProducerRecord. topic (str "msg-" i))))
    (println "send ループ終了:" (/ (- (System/nanoTime) start) 1e6) "ms")
    (when (= mode "close")
      (.close p)
      (println "close 完了:" (/ (- (System/nanoTime) start) 1e6) "ms"))
    (System/exit 0)))
