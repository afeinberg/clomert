;; This is the stores.xml to use for this example:
;; <stores>
;; <store>
;;     <name>person</name>
;;     <persistence>bdb</persistence>
;;     <routing>client</routing>
;;     <replication-factor>1</replication-factor>
;;     <required-reads>1</required-reads>
;;     <required-writes>1</required-writes>
;;     <key-serializer>
;;         <type>string</type>
;;     </key-serializer>
;;     <value-serializer>
;;         <type>json</type>
;;         <schema-info version="1">
;;             {"first":"string", "last": "string", "zipcode": "int32"}
;;         </schema-info>
;;     </value-serializer>
;;  </store>
;;  <store>
;;     <name>zipcode</name>
;;     <persistence>bdb</persistence>
;;     <routing>client</routing>
;;     <replication-factor>1</replication-factor>
;;     <required-reads>1</required-reads>
;;     <required-writes>1</required-writes>
;;     <key-serializer>
;;         <type>json</type>
;;         <schema-info version="1">"int32"</schema-info>
;;     </key-serializer>
;;     <value-serializer>
;;         <type>json</type>
;;         <schema-info version="1">["string"]</schema-info>
;;     </value-serializer>
;; </store>
;; </stores>

(ns zipcode-example
  (require [clomert :as v]))

(defn main []
  (with-open [factory
              (v/make-socket-store-client-factory
               (v/make-client-config {:bootstrap-urls "tcp://localhost:6666"}))]
    (let [client-zipcode (v/make-store-client factory "zipcode")
          client-person (v/make-store-client factory "person")]

      ;; shows the do-store macro
      (v/do-store client-person
                  (:put  "alex.feinberg" { "first" "alex",
                                     "last" "feinberg",
                                     "zipcode" 94041 })
                  (:put "joe.schmoe" { "first" "joe",
                                                "last" "schmoe",
                                                "zipcode" 94041 }))

      ;; regular store functions
      (v/store-put
       client-zipcode (get (v/versioned-value (v/store-get
                                               client-person
                                               "alex.feinberg"))
                           "zipcode")
       ["alex.feinberg"])

      ;; use an optimistic lock to mutate a list inside a value
      (v/store-apply-update
       client-zipcode
       (fn [client]
         (let [ver (v/store-get client 94041)
               val (v/versioned-value ver)]
           (v/store-conditional-put client
                                    94041
                                    (v/versioned-set-value! ver (cons
                                                                 "joe.schmoe"
                                                                 val))))))
      ;; use a get-all
      (doseq  [[_ person-versioned]
               (v/store-get-all client-person
                                (v/versioned-value (v/store-get
                                                    client-zipcode
                                                    94041)))]
        (println (get (v/versioned-value person-versioned)
                      "first")
                 "lives in 94041")))))








