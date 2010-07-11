(ns clomert
  (:import (voldemort.client
            ClientConfig
            StoreClientFactory
            SocketStoreClientFactory
            StoreClient
            UpdateAction)
           (voldemort.versioning Version Versioned VectorClock)))

;; factory functionality

(def *props-map*
     {:bootstrap-urls
      (. ClientConfig BOOTSTRAP_URLS_PROPERTY),
      :connection-timeout-ms
      (. ClientConfig CONNECTION_TIMEOUT_MS_PROPERTY),
      :enable-jmx
      (. ClientConfig ENABLE_JMX_PROPERTY),
      :failure-detector-async-recovery-interval
      (. ClientConfig FAILUREDETECTOR_ASYNCRECOVERY_INTERVAL_PROPERTY),
      :failure-detector-bannage-period
      (. ClientConfig FAILUREDETECTOR_BANNAGE_PERIOD_PROPERTY),
      :failure-detector-catastrophic-error-types
      (. ClientConfig FAILUREDETECTOR_CATASTROPHIC_ERROR_TYPES_PROPERTY),
      :failure-detector-implementation
      (. ClientConfig FAILUREDETECTOR_IMPLEMENTATION_PROPERTY),
      :failure-detector-request-length-threshold
      (. ClientConfig FAILUREDETECTOR_REQUEST_LENGTH_THRESHOLD_PROPERTY),
      :failure-detector-threshold-count-minimum
      (. ClientConfig FAILUREDETECTOR_THRESHOLD_COUNTMINIMUM_PROPERTY),
      :failure-detector-threshold-interval
      (. ClientConfig FAILUREDETECTOR_THRESHOLD_INTERVAL_PROPERTY),
      :failure-detector-treshold
      (. ClientConfig FAILUREDETECTOR_THRESHOLD_PROPERTY),
      :max-bootstrap-retries
      (. ClientConfig MAX_BOOTSTRAP_RETRIES),
      :max-connections-per-node
      (. ClientConfig MAX_CONNECTIONS_PER_NODE_PROPERTY),
      :max-queued-requests
      (. ClientConfig MAX_QUEUED_REQUESTS_PROPERTY),
      :max-threads
      (. ClientConfig MAX_THREADS_PROPERTY),
      :max-total-connections
      (. ClientConfig MAX_TOTAL_CONNECTIONS_PROPERTY),
      :node-bannage-ms
      (. ClientConfig MAX_TOTAL_CONNECTIONS_PROPERTY),
      :request-format
      (. ClientConfig REQUEST_FORMAT_PROPERTY),
      :routing-timeout-ms
      (. ClientConfig ROUTING_TIMEOUT_MS_PROPERTY),
      :serializer-factory-class
      (. ClientConfig SERIALIZER_FACTORY_CLASS_PROPERTY),
      :socket-buffer-size
      (. ClientConfig SOCKET_BUFFER_SIZE_PROPERTY),
      :socket-keepalive
      (. ClientConfig SOCKET_KEEPALIVE_PROPERTY),
      :socket-timeout-ms
      (. ClientConfig SOCKET_TIMEOUT_MS_PROPERTY),
      :thread-idle-ms
      (. ClientConfig THREAD_IDLE_MS_PROPERTY) 
      })
                   
(defn make-client-config
  ([config-map]
     (let [props (new java.util.Properties)]
       (doseq [[k v] config-map]
         (doto props
           (.setProperty (get *props-map* k) (.toString v))))
       (new ClientConfig props))))

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

(defn store-preflist [key]
  "Get a preference list of responsible nodes for a key."
  (.getResponsibleNodes key))


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
  (map (fn [form] 
         `(store-do-op ~store ~@form))
       forms))
    

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



                    
  
  