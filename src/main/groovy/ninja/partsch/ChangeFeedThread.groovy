package ninja.partsch

import com.rethinkdb.gen.ast.Changes
import com.rethinkdb.gen.exc.ReqlDriverError
import com.rethinkdb.net.Connection
import com.rethinkdb.net.Cursor
import ninja.partsch.exceptions.ChangeFeedException

import java.util.concurrent.CancellationException
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class ChangeFeedThread extends Thread {

    Map<String, Closure> typeHandlerMap
    Cursor changeFeed
    Connection dbConn
    Phaser readyPhaser

    def ChangeFeedThread(Map<String, Closure> typeHandlerMap, Connection dbConn, Changes changes) {
        if(typeHandlerMap == null)
            throw new IllegalArgumentException("cannot create ChangeFeedThread with null typeHandlerMap")
        this.typeHandlerMap = typeHandlerMap
        if(dbConn == null)
            throw new IllegalArgumentException("cannot create ChangeFeedThread with null dbConn")
        this.dbConn = dbConn
        if(changes == null)
            throw new IllegalArgumentException("cannot create ChangeFeedThread with null changes cursor")
        this.changeFeed = changes.run(dbConn)
        this.changeFeed = changeFeed
        this.readyPhaser = new Phaser(2)
    }

    public void run() {
        try {
            for (feedItem in changeFeed) {
                def handler = typeHandlerMap.get(feedItem["type"])
                def updated = false
                switch (feedItem["type"]) {
                    case "add":
                    case "change":
                    case "initial":
                        if (handler != null)
                            handler(feedItem["new_val"], feedItem["old_val"])
                        updated = true
                        break;
                    case "remove":
                        if (handler != null)
                            handler(feedItem["old_val"])
                        updated = true
                        break;
                    case "state":
                        if (handler != null)
                            handler(feedItem["state"])
                        if(feedItem["state"] == "ready")
                            readyPhaser.arrive()
                        break;
                }
                // document initialized, added, changed or removed
                if (updated) {
                    def updateHandler = typeHandlerMap.get "update"
                    if (updateHandler != null)
                        updateHandler(feedItem["type"], feedItem["new_val"], feedItem["old_val"])
                }
                // new document added
                if (feedItem["type"] == "initial" || feedItem["type"] == "add") {
                    def newHandler = typeHandlerMap.get "new"
                    if (newHandler != null)
                        newHandler(feedItem["type"], feedItem["new_val"])
                }
            }
        } catch(ReqlDriverError rde) {
            // if feed was not canceled, rethrow
            if(!(rde.cause instanceof CancellationException))
                throw rde
        }
    }

    def stopFeed() {
        dbConn.close()
    }

    def awaitReady(timeout = 5000) {
        try {
            readyPhaser.arrive()
            readyPhaser.awaitAdvanceInterruptibly(0, timeout, TimeUnit.MILLISECONDS)
        } catch(TimeoutException te) {
            throw new ChangeFeedException("awaiting change feeed for ready state timed out after ${timeout} milliseconds", te)
        }
    }

}
