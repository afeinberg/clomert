(ns clomert
  (:import (voldemort.client
            ClientConfig
            StoreClientFactory
            SocketStoreClientFactory
            StoreClient
            UpdateAction)
           (voldemort.versioning Version Versioned VectorClock)))

;; factory functionality
;; TODO: provide an idiomatic wrapper for client config

(defn socket-store-client-factory
  "Create a socket store client factory."
  ([urls] (socket-store-client-factory (new ClientConfig) urls))
  ([#^ClientConfig client-config urls]
     (new SocketStoreClientFactory (doto client-config
                                     (.setBootstrapUrls urls)))))

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
     (.get client key)))

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
  ([#^StoreClient client value]
     (.delete client value)))

(defn store-conditional-delete
  "Conditional delete: delete only a specific version."
  ([#^StoreClient client #^Versioned value]
     (.delete client value)))

(defn store-apply-update
  "Perform an update inside an optimistic lock."
  ([#^StoreClient client update-fn]
     (apply-update 3 client update-fn))
  
  ([#^Integer max-tries #^StoreClient client update-fn]
     (apply-update max-tries client update-fn (fn [])))
  
  ([#^Integer max-tries #^StoreClient client update-fn rollback-fn]
     (.applyUpdate client
                   (proxy [UpdateAction] []
                     (update [#^StoreClient x]
                             (update-fn x))
                     (rollback []
                               (rollback-fn)))
                   max-tries)))

(defn store-preflist [key]
  "Get a preference list of responsible nodes for a key."
  (.getResponsibleNodes key))

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
  
(defn vector-clock-increment
  "Increment a vector clock."
  ([#^VectorClock version #^Integer node]
     (increment-version version node (System/currentTimeMillis)))
  ([#^VectorClock version #^Integer node #^Long ts]
     (doto version
       (.incrementVersion node ts))))



                    
  
  