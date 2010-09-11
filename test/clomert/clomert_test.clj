(ns clomert.clomert_test
  (:import (voldemort.client MockStoreClientFactory)
           (voldemort.serialization StringSerializer))
  (require [clomert :as v])
  (:use clojure.test))

(def *serializer* (new StringSerializer))
(def *factory* (new MockStoreClientFactory *serializer* *serializer*))

(deftest make-store-client
  (let [client (v/make-store-client *factory* "test")]
    (is
     (= "world"
      (v/do-store client
                  (:put "hello" "world")
                  (:get-value "hello"))))))

(deftest store-get-all
  (let [client (v/make-store-client *factory* "test")
        res (v/do-store client
                        (:put "russia" "moscow")
                        (:put "belarus" "minsk")
                        (:put "ukraine" "kiev")
                        (:get-all ["russia" "belarus" "ukraine"]))]
    (is
     (and (= (v/versioned-value (get res "russia")) "moscow")
          (= (v/versioned-value (get res "belarus")) "minsk")
          (= (v/versioned-value (get res "ukraine")) "kiev")))))

(deftest store-apply-update
  (let [client (v/make-store-client *factory* "test")]
    (v/store-apply-update
     5 client
     (fn [#^StoreClient scl]
       (let [v (v/store-get scl "foo")]
         (v/store-conditional-put scl "foo"
                                  (if (nil? v)
                                    (v/make-versioned "a")
                                    (v/versioned-set-value!
                                     (v (str (v/versioned-value v)
                                             "b"))))))))))
