package ninja.partsch

import com.rethinkdb.gen.ast.Changes
import com.rethinkdb.gen.ast.ReqlExpr

import java.util.concurrent.ConcurrentHashMap

class AsyncChangeFeed {

    Changes changeFeed
    Closure connectionFactory
    Map<String, Closure> typeHandlerMap = new ConcurrentHashMap<>()

    def AsyncChangeFeed(ReqlExpr query, Closure connFac) {
        // create change feeed from query
        if(query == null)
            throw new IllegalArgumentException("cannot create change feed from null query")
        changeFeed = query.changes()
                .optArg("include_initial", true)
                .optArg("include_states", true)
                .optArg("include_types", true)
                .optArg("squash", true)
        // set connection factory
        if(connFac == null)
            throw new IllegalArgumentException("connection factory must not be null")
        connectionFactory = connFac
    }


    def onAdd(Closure handler) {
        typeHandlerMap.put("add", handler)
        this
    }

    def onChange(Closure handler) {
        typeHandlerMap.put("change", handler)
        this
    }

    def onInitial(Closure handler) {
        typeHandlerMap.put("initial", handler)
        this
    }

    def onRemove(Closure handler) {
        typeHandlerMap.put("remove", handler)
        this
    }

    def onNew(Closure handler) {
        typeHandlerMap.put("new", handler)
        this
    }

    def onUpdate(Closure handler) {
        typeHandlerMap.put("update", handler)
        this
    }

    def onStateChange(Closure handler) {
        typeHandlerMap.put("state", handler)
        this
    }

    def startFeed() {
        def dbConn = connectionFactory()
        Thread.start {
            def cursor = changeFeed.run(dbConn)
            for(feedItem in cursor) {
                def handler = typeHandlerMap.get(feedItem["type"])
                def updated = false
                switch(feedItem["type"]) {
                    case "add":
                    case "change":
                    case "initial":
                        if(handler != null)
                            handler(feedItem["new_val"], feedItem["old_val"])
                        updated = true
                        break;
                    case "remove":
                        if(handler != null)
                            handler(feedItem["old_val"])
                        updated = true
                        break;
                    case "state":
                        if(handler != null)
                            handler(feedItem["state"])
                        break;
                }
                // document initialized, added, changed or removed
                if(updated) {
                    def updateHandler = typeHandlerMap.get "update"
                    if(updateHandler != null)
                        updateHandler(feedItem["type"], feedItem["new_val"], feedItem["old_val"])
                }
                // new document added
                if(feedItem["type"] == "initial" || feedItem["type"] == "add") {
                    def newHandler = typeHandlerMap.get "new"
                    if(newHandler != null)
                        newHandler(feedItem["type"], feedItem["new_val"])
                }
            }
        }
    }

}
