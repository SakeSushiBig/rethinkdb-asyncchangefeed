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
        def thread = new ChangeFeedThread(typeHandlerMap, connectionFactory(), changeFeed)
        thread.start()
        return thread
    }

}
