;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.common.types.component
  (:require
    [app.common.types.container :as ctn]))

(defn instance-root?
  "An intance root is the shape, inside an instance, that has
  the link to the component. Other shapes have :shape-ref but
  not :component-id."
  [shape]
  (some? (:component-id shape)))
 
(defn instance-of?
  "Check if the shape is the root of an instance of the
  given component at the given file library."
  [shape file-id component-id]
  (and (some? (:component-id shape))
       (some? (:component-file shape))
       (= (:component-id shape) component-id)
       (= (:component-file shape) file-id)))

(defn is-main-of?
  "Check if the first shape is a near or remote main of the second one."
  [shape-main shape-inst]
  (and (:shape-ref shape-inst)
       (or (= (:shape-ref shape-inst) (:id shape-main))
           (= (:shape-ref shape-inst) (:shape-ref shape-main)))))

(defn is-main-instance?
  "Check if the shape in the page is the main instance of the component."
  [shape-id page-id component]
  (and (= shape-id (:main-instance-id component))
       (= page-id (:main-instance-page component))))

(defn get-component-root
  "Get the root shape of the component."
  [component]
  (get-in component [:objects (:id component)]))

(defn uses-library-components?
  "Check if the shape uses any component in the given library."
  [shape library-id]
  (and (some? (:component-id shape))
       (= (:component-file shape) library-id)))

(defn standalone-instance?
  "Check if the shape inside the container is not a subinstance
  (an instance inside another one)."
  [shape container]
  (when (:component-id shape)
    (if (ctn/page? container)
      (:component-root? shape)
      (nil? (:parent-id shape)))))
