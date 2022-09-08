;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns user
  (:require
   [app.srepl.helpers]
   [app.common.data :as d]
   [app.common.exceptions :as ex]
   [app.common.geom.matrix :as gmt]
   [app.common.perf :as perf]
   [app.common.pprint :as pp]
   [app.common.transit :as t]
   [app.common.uuid :as uuid]
   [app.config :as cfg]
   [app.main :as main]
   [app.util.blob :as blob]
   [app.util.fressian :as fres]
   [app.util.json :as json]
   [app.util.time :as dt]
   [clj-async-profiler.core :as prof]
   [clojure.contrib.humanize :as hum]
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint print-table]]
   [clojure.repl :refer :all]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as sgen]
   [clojure.test :as test]
   [clojure.tools.namespace.repl :as repl]
   [clojure.walk :refer [macroexpand-all]]
   [datoteka.core]
   [integrant.core :as ig]))

(repl/disable-reload! (find-ns 'integrant.core))
(set! *warn-on-reflection* true)

(defonce system nil)

;; --- Benchmarking Tools

(defmacro run-quick-bench
  [& exprs]
  `(with-progress-reporting (quick-bench (do ~@exprs) :verbose)))

(defmacro run-quick-bench'
  [& exprs]
  `(quick-bench (do ~@exprs)))

(defmacro run-bench
  [& exprs]
  `(with-progress-reporting (bench (do ~@exprs) :verbose)))

(defmacro run-bench'
  [& exprs]
  `(bench (do ~@exprs)))

;; --- Development Stuff

(defn- run-tests
  ([] (run-tests #"^app.*-test$"))
  ([o]
   (repl/refresh)
   (cond
     (instance? java.util.regex.Pattern o)
     (test/run-all-tests o)

     (symbol? o)
     (if-let [sns (namespace o)]
       (do (require (symbol sns))
           (test/test-vars [(resolve o)]))
       (test/test-ns o)))))

(defn- start
  []
  (alter-var-root #'system (fn [sys]
                             (when sys (ig/halt! sys))
                             (-> main/system-config
                                 (ig/prep)
                                 (ig/init))))
  :started)

(defn- stop
  []
  (alter-var-root #'system (fn [sys]
                             (when sys (ig/halt! sys))
                             nil))
  :stoped)

(defn restart
  []
  (stop)
  (repl/refresh :after 'user/start))

(defn restart-all
  []
  (stop)
  (repl/refresh-all :after 'user/start))

(defn compression-bench
  [data]
  (let [humanize (fn [v] (hum/filesize v :binary true :format " %.4f "))]
    (print-table
     [{:v1 (humanize (alength (blob/encode data {:version 1})))
       :v2 (humanize (alength (blob/encode data {:version 2})))
       :v3 (humanize (alength (blob/encode data {:version 3})))
       :v4 (humanize (alength (blob/encode data {:version 4})))
       }])))

(defonce debug-tap
  (do
    (add-tap #(locking debug-tap
                (prn "tap debug:" %)))
    1))


(defn analyze-file
  ([file] (analyze-file file {}))
  ([{:keys [data] :as file} state]
   (let [total-pages (-> data :pages count)
         page-sizes  (->> (vals (:pages-index data))
                          (map app.util.fressian/encode)
                          (map alength)
                          (map float))
         obj-sizes   (->> (vals (:pages-index data))
                          (mapcat :objects)
                          (map second)
                          (map app.util.fressian/encode)
                          (map alength)
                          (map float))
         file-size   (-> data app.util.fressian/encode alength float)
         ]

     (-> state
         (update :total-files (fnil inc 0))
         (update :total-pages (fnil + 0) total-pages)
         (update :total-objs (fnil + 0) (count obj-sizes))
         (update :max-file-size (fnil max ##-Inf) file-size)
         (update :min-file-size (fnil min ##Inf) file-size)
         (update :max-page-size (fnil max ##-Inf) (apply max page-sizes))
         (update :min-page-size (fnil min ##Inf) (apply min page-sizes))
         (update :max-obj-size (fnil max ##-Inf) (apply max obj-sizes))
         (update :min-obj-size (fnil min ##Inf) (apply min obj-sizes))
         (update :total-file-size (fnil + 0) file-size)
         (update :total-page-size (fnil + 0) (reduce + 0 page-sizes))
         (update :total-obj-size (fnil + 0) (reduce + 0 obj-sizes))))))


(defn print-summary
  [state]
  (let [fmt-size (fn [v] (format "%.1f KiB" (quot v 1024.0)))]
    (println "=========== SUMMARY ===========")
    (println "=> total analized files:  " (:total-files state))
    (println "=> total analized pages:  " (:total-pages state))
    (println "=> total analized objects:" (:total-objs state))
    (println "")
    (println "=> max file size:         " (fmt-size (:max-file-size state)))
    (println "=> max page size:         " (fmt-size (:max-page-size state)))
    (println "=> max obj size:          " (fmt-size (:max-obj-size state)))
    (println "=> min file size:         " (fmt-size (:min-file-size state)))
    (println "=> min page size:         " (fmt-size (:min-page-size state)))
    (println "=> min obj size:          " (fmt-size (:min-obj-size state)))

    (println "=> avg file size:         " (fmt-size (quot (:total-file-size state)
                                                          (:total-files state))))
    (println "=> avg page size:         " (fmt-size (quot (:total-page-size state)
                                                          (:total-pages state))))
    (println "=> avg obj size:          " (fmt-size (quot (:total-obj-size state)
                                                          (:total-objs state))))
    (println "")
    (println "=> avg pages/file:        " (quot (:total-pages state)
                                                (:total-files state)))))


;; (defn print-summary
;;   [state]
;;   (let [hum (fn [v] (clojure.contrib.humanize/filesize v :binary true :format "%.4f "))]
;;     (clojure.pprint/print-table
;;      [{:total-files (:total-files state)
;;        :total-pages (:total-pages state)
;;        :total-objs  (:total-objs state)

;;        :max-file-size (-> state :max-file-size hum)
;;        :min-file-size (-> state :min-file-size hum)
;;        :avg-file-size (hum (quot (:total-file-size state)
;;                                  (:total-files state)))

;;        :avg-pages-per-file (quot (:total-pages state)
;;                                  (:total-files state))

;;        :max-page-size (-> state :max-page-size hum)
;;        :min-page-size (-> state :min-page-size hum)
;;        :avg-page-size (hum (quot (:total-page-size state)
;;                                  (:total-pages state)))

;;        :max-obj-size (-> state :max-obj-size hum)
;;        :min-obj-size (-> state :min-obj-size hum)
;;        :avg-obj-size (hum (quot (:total-obj-size state)
;;                                 (:total-objs state)))
;;        }]
;;      )))




;; (defn test-bench
;;   [file]
;;   (println "encoding all file")
;;   (time
;;    (blob/encode (:data file)))

;;   (doseq [[id data] (-> file :data :pages-index)]
;;     (println "encoding file" id)
;;     (time (blob/encode data)))

;;   nil)


(def file-as-is
  (delay (app.srepl.helpers/get-file system #uuid "00050006-17c4-8000-893e-77588711a68d")))

(def file-with-blobs
  (delay
    (-> @file-as-is
        (update-in [:data :pages-index]
                   (fn [index]
                     (reduce-kv (fn [obj k v]
                                  (assoc obj k (update v :objects fres/encode)))
                                {}
                                index)))
        (update-in [:data :components]
                   (fn [index]
                     (reduce-kv (fn [obj k v]
                                  (assoc obj k (update v :objects fres/encode)))
                                {}
                                index))))))


(def test1
  (delay
    (let [obj1 (penpot.ConstantMap/createEmpty blob/encode blob/decode)
          obj2 (.set obj1 (uuid/custom 0 1) {:foo 1})
          obj3 (.set obj2 (uuid/custom 0 2) {:bar 1})]
      obj3)))
