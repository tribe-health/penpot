;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) UXBOX Labs SL

(ns app.util.file-maps
  "A penpot file specific Map implementations."
  (:require
   [app.util.fressian :as fres])
  (:import
   clojure.lang.AMapEntry
   clojure.lang.IFn
   clojure.lang.IHashEq
   clojure.lang.Counted
   clojure.lang.IMapEntry
   clojure.lang.IObj
   clojure.lang.IPersistentCollection
   clojure.lang.IPersistentMap
   clojure.lang.ISeq
   clojure.lang.MapEntry
   clojure.lang.Murmur3
   clojure.lang.PersistentArrayMap
   clojure.lang.PersistentHashMap
   clojure.lang.RT
   clojure.lang.Util
   java.nio.ByteBuffer
   java.util.HashMap
   java.util.Iterator
   java.util.LinkedList
   java.util.UUID))

(set! *warn-on-reflection* true)

(def ^:const RECORD_SIZE (+ 16 8))
(def ^:const POSITION_MASK 0x00000000ffffffff)
(def ^:const SIZE_MASK 0xffffffff00000000)

(definterface IUUIDtoObjectMap
  (initialize [])
  (compact [])
  (toByteArray [])
  (getHashEq [key]))

(defn- get-size-from-rc
  [^long rc]
  (bit-shift-right rc 32))

(defn- get-position-from-rc
  [^long rc]
  (bit-and rc POSITION_MASK))

(deftype UUIDtoObjectMapEntry [^IUUIDtoObjectMap cmap ^UUID key]
  IMapEntry
  (key [_] key)
  (getKey [_] key)

  (val [_]
    (get cmap key))
  (getValue [_]
    (get cmap key))

  IHashEq
  (hasheq [_]
    (.getHashEq cmap key)))

(deftype UUIDtoObjectMapIterator [^Iterator iterator ^IUUIDtoObjectMap cmap]
  Iterator
  (hasNext [_]
    (.hasNext iterator))

  (next [_]
    (let [entry (.next iterator)]
      (UUIDtoObjectMapEntry. cmap (key entry)))))

(deftype UUIDtoObjectMap [^:unsynchronized-mutable metadata
                          ^:unsynchronized-mutable hash
                          ^:unsynchronized-mutable positions
                          ^:unsynchronized-mutable cache
                          ^:unsynchronized-mutable blob
                          ^:unsynchronized-mutable header
                          ^:unsynchronized-mutable content
                          ^:unsynchronized-mutable initialized?
                          modified?]
  IUUIDtoObjectMap
  (initialize [_]
    (when-not initialized?
      (let [header-size (.getInt ^ByteBuffer blob 0)
            header'     (.slice ^ByteBuffer blob 4 header-size)
            content'    (.slice ^ByteBuffer blob
                                (int (+ 4 header-size))
                                (int (- (.remaining ^ByteBuffer blob)
                                        (+ 4 header-size))))
            nitems      (/ (.remaining ^ByteBuffer blob) RECORD_SIZE)
            positions'  (reduce (fn [positions i]
                                  (let [hb (.slice ^ByteBuffer header'
                                                   (int (* i RECORD_SIZE))
                                                   (int RECORD_SIZE))
                                        ra (.getLong ^ByteBuffer hb)
                                        rb (.getLong ^ByteBuffer hb)
                                        rc (.getLong ^ByteBuffer hb)]
                                    (assoc! positions (UUID. ^long ra ^long rb) rc)))
                                (transient {})
                                (range nitems))]
        (set! positions (persistent! positions'))
        (set! cache {})
        (set! header header')
        (set! content content')
        (set! initialized? true))))

  (compact [_]
    #_(when modified?
      (loop [entries (seq positions)
             size    0
             elems   0
             items   {}
             hashs   {}]
        (if-let [entry (first entries)]
          (let [k  (key entry)
                rc (val entry)]
            (if (= rc -1)
              (recur (rest entries)
                     (+ size (get-size-from-rc rc))
                     (inc elems)
                     items
                     hashs)))))))

  (toByteArray [self]
    (.compact self)
    (.array ^ByteBuffer blob))

  (getHashEq [self key]
    (.initialize self)
    (if (contains? cache key)
      (hash (get cache key))
      (let [rc  (get positions key)
            pos (get-position-from-rc rc)]
        (.getInt ^ByteBuffer content (int pos)))))

  Cloneable
  (clone [_]
    (if initialized?
      (UUIDtoObjectMap. metadata hash positions cache blob header content initialized? modified?)
      (UUIDtoObjectMap. metadata nil nil nil blob nil nil false false)))

  IObj
  (meta [_] metadata)

  (withMeta [self meta]
    (set! metadata meta)
    self)

  IPersistentMap
  (containsKey [self key]
    (.initialize self)
    (contains? positions key))

  (entryAt [self key]
    (.initialize self)
    (UUIDtoObjectMapEntry. self key))

  (valAt [self key]
    (.initialize self)

    (if (contains? cache key)
      (get cache key)
      (if (contains? positions key)
        (let [rc   (get positions key)
              size (get-size-from-rc rc)
              pos  (get-position-from-rc rc)
              tmp  (byte-array (- size 4))]
          (.get ^ByteBuffer content (int (+ pos 4)) ^bytes tmp (int 0) (int (- size 4)))
          (let [val (fres/decode tmp)]
            (set! cache (assoc cache key val))
            val))
        (do
          (set! cache (assoc cache key nil))
          nil))))

  (assoc [self key val]
    (.initialize self)
    (UUIDtoObjectMap. metadata
                      nil
                      (assoc positions key -1)
                      (assoc cache key val)
                      blob
                      header
                      content
                      initialized?
                      true))

  (assocEx [self key val]
    (throw (UnsupportedOperationException. "method not implemented")))

  (without [self key]
    (.initialize self)
    (UUIDtoObjectMap. metadata
                      nil
                      (dissoc positions key)
                      (dissoc cache key)
                      blob
                      header
                      content
                      initialized?
                      true))

  (valAt [self key not-found]
    (.initialize self)
    (if (.containsKey ^IPersistentMap positions key)
      (.valAt self key)
      not-found))

  Counted
  (count [_]
    (count positions))

  Iterable
  (iterator [self]
    #_(UUIDtoObjectMapIterator. self))

  )


