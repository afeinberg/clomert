(ns clomert
  (:import (voldemort.client
            ClientConfig
            StoreClientFactory
            SocketStoreClientFactory
            StoreClient
            UpdateAction)
           (voldemort.versioning Version Versioned VectorClock)))

(def rewrite-keyword
  (memoize (fn [#^Keyword kw]
             (str (.replace
                   (.toUpperCase
                    (.substring (str kw) 1))
                   "-"
                   "_")
                  "_PROPERTY"))))

(defn mk-key [kw]
  (eval (symbol (str "ClientConfig/" (rewrite-keyword kw)))))

(defn props-from-map [config]
  (doto (java.util.Properties.)
    (.putAll
     (into {}
	   (map (fn [[k v]]
		  (mk-key k) (str v))
		config)))))

(defn make-client-config [config]
  (ClientConfig. (props-from-map config)))

(defn socket-store-client-factory
  "Create a socket store client factory [Deprecated]."
  ([urls] (socket-store-client-factory (new ClientConfig) urls))
  ([#^ClientConfig client-config urls]
     (new SocketStoreClientFactory (doto client-config
                                     (.setBootstrapUrls urls)))))

(defmulti make-socket-store-client-factory
  "Create a socket store client factory."
  class)

(defmethod make-socket-store-client-factory ClientConfig [c]
           (new SocketStoreClientFactory c))

(defmethod make-socket-store-client-factory :default [urls]
           (new SocketStoreClientFactory (make-client-config
                                          {:bootstrap-urls urls})))
  
(defn make-store-client [#^StoreClientFactory factory store-name]
  "Create a store client from a factory."
  (.getStoreClient factory store-name))

;; basic store client functions

(defn store-put
  "Unconditional put, clobber the existing value if set."
  ([#^StoreClient client key value]
     (.put client key value)))

(defn store-conditional-put
  "Conditional put, if version is obsolete throw ObsoleteVersionException."
  ([#^StoreClient client key #^Versioned value]
     (.put client key value)))

(defn store-put-if-not-obsolete
  "Like conditional-put, but return false instead of throwing an exception."
  ([#^StoreClient client key #^Versioned value]
     (.putIfNotObsolete client key value)))

(defn store-get
  "Get a value and its version from a store."
  ([#^StoreClient client key]
     (.get client key))
  ([#^StoreClient client key #^Versioned default]
     (.get client key default)))

(defn store-get-all
  "Perform a multi-get on a store, return map key => (version, value)."
  ([#^StoreClient client values]
     (.getAll client values)))

(defn store-get-value
  "Get a value from a store."
  ([#^StoreClient client key]
     (.getValue client key))
  ([#^StoreClient client key default]
     (.getValue client key default)))

(defn store-delete
  "Delete a value."
  ([#^StoreClient client key]
     (.delete client key))
  ([#^StoreClient client key #^Version version]
     (.delete client key version)))
 
(defn store-apply-update
  "Perform an update inside an optimistic lock."
  ([#^StoreClient client update-fn]
     (store-apply-update 3 client update-fn))
  
  ([#^Integer max-tries #^StoreClient client update-fn]
     (store-apply-update max-tries client update-fn (fn [])))
  
  ([#^Integer max-tries #^StoreClient client update-fn rollback-fn]
     (.applyUpdate client
                   (proxy [UpdateAction] []
                     (update [#^StoreClient x]
                             (update-fn x))
                     (rollback []
                               (rollback-fn)))
                   max-tries)))

(defn store-preflist [client key]
  "Get a preference list of responsible nodes for a key."
  (.getResponsibleNodes client key))


(defn store-do-op [#^StoreClient store op & args]
  (cond (= op :put)
        (let [key (first args)
              value (second args)]
          (store-put store key value))
        (= op :conditional-put)
        (let [key (first args)
              #^Versioned value (second args)]
          (store-conditional-put store key value))
        (= op :put-if-not-obsolete)
        (let [key (first args)
              #^Versioned value (second args)]
          (store-put-if-not-obsolete store key value))
        (= op :get)
        (let [key (first args)]
          (store-get store key))
        (= op :get-all)
        (let [keys (first args)]
          (store-get-all store keys))
        (= op :get-value)
        (let [key (first args)
              default (second args)]
          (if (nil? default)
            (store-get-value store key)
            (store-get-value store key default)))
        (= op :delete)
        (let [key (first args)
              version (second args)]
          (if (nil? version)
            (store-delete store key)
            (store-delete store key version)))
        true
        (throw (new IllegalArgumentException (str "No such operation" op)))))

(defmacro do-store [#^StoreClient store & forms]
  (let [forms-rewritten (map (fn [form] 
                               `(store-do-op ~store ~@form))
                             forms)]
    `(do ~@forms-rewritten)))

;; versions and vector clocks

(defn make-versioned
  "Create a new (version, value) tupple."
  ([object] (new Versioned object))
  ([object #^Version v]
     (new Versioned object v)))

(defn versioned-value [#^Versioned v]
  "Get the value of a (version, value) tupple."
  (.getValue v))

(defn versioned-set-value! [#^Versioned v obj]
  "Mutate the value in (version, value) tupple."
  (doto v
    (.setObject obj)))
  
(defn vector-clock-increment!
  "Mutably increment a vector clock."
  ([#^VectorClock version #^Integer node]
     (vector-clock-increment! version node (System/currentTimeMillis)))
  ([#^VectorClock version #^Integer node #^Long ts]
     (doto version
       (.incrementVersion node ts))))

(defn vector-clock-incremented
  "Make an incremented copy of a vector clock."
  ([#^VectorClock version #^Integer node]
     (vector-clock-incremented version node (System/currentTimeMillis)))
  ([#^VectorClock version #^Integer node #^Long ts]
     (.incremented version node ts)))



                    
  
  