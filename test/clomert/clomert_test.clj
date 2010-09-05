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

  