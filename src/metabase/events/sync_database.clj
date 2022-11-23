(ns metabase.events.sync-database
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase.events :as events]
            [metabase.models.database :refer [Database]]
            [metabase.sync :as sync]
            [metabase.sync.sync-metadata :as sync-metadata]
            [metabase.util :as u]
            [metabase.sync.util :as sync-util]
            [metabase.util.i18n :refer [trs]]))

(def ^:const sync-database-topics
  "The `Set` of event topics which are subscribed to for use in database syncing."
  #{:database-create
    ;; published by POST /api/database/:id/sync -- a message to start syncing the DB right away
    :database-trigger-sync})

(defonce ^:private ^{:doc "Channel for receiving event notifications we want to subscribe to for database sync events."}
  sync-database-channel
  (a/chan))


;;; ------------------------------------------------ EVENT PROCESSING ------------------------------------------------


(defn process-sync-database-event
  "Handle processing for a single event notification received on the `sync-database-channel`"
  [{topic :topic, object :item, :as event}]
  ;; try/catch here to prevent individual topic processing exceptions from bubbling up.  better to handle them here.
  (try
    (when event
      (when-let [database (Database (events/object->model-id topic object))]
        ;; just kick off a sync on another thread
        (future
          (try
            (let [engine    (:engine database)] (
                 (log/warn (trs "database map is {0}, engine is {1}" database engine))
                 (log/warn (trs "engine is = sparksql {0}" (= engine "sparksql")))
                 (log/warn (trs "engine is equal sparksql {0}" (.equals engine "sparksql")))
                 (log/warn (trs "engine str is = sparksql {0}" (= (str engine) "sparksql")))
                 (log/warn (trs "engine str compare sparksql {0}" (= 0 (compare (str engine) "sparksql"))))
                 (log/warn (trs "engine str is equal sparksql {0}" (.equals (str engine) "sparksql")))
                 (log/warn (trs "engine type is {0}" (type engine)))
                 (log/warn (trs "engine str is {0}" (str engine)))
                 (log/warn (trs "engine str type is {0}" (type (str engine))))
                 (log/warn (trs "sparksql type is {0}" (type "sparksql")))
            (if (= (str engine) "sparksql")
            (do (log/warn (trs "find spark sql engine, make it complete. database: {0}" database))
                (log/warn (trs "find spark sql engine, make it complete. :engine: {0}" :engine ))
                (sync-util/set-initial-database-sync-complete! database))
            ;; only do the 'full' sync if this is a "full sync" database. Otherwise just do metadata sync only
            (if (:is_full_sync database)
              (sync/sync-database! database)
              (sync-metadata/sync-db-metadata! database)))))
          (catch Throwable e
              (log/error e (trs "Error syncing Database {0}" (u/the-id database))))))))
    (catch Throwable e
      (log/warn e (trs "Failed to process sync-database event.") topic))))


;;; ---------------------------------------------------- LIFECYLE ----------------------------------------------------

(defmethod events/init! ::Sync
  [_]
  (events/start-event-listener! sync-database-topics sync-database-channel process-sync-database-event))
