![RethinkDB Logo](https://raw.githubusercontent.com/docker-library/docs/af9f91fe186f3ea3afee511d0a53b50088fdc381/rethinkdb/logo.png) + Groovy!

# RethinkDB Async Change Feeds for Groovy

A groovy library providing asynchronous access to RethinkDB's changefeeds using the official Java Driver. Here an example:

```groovy
def changeFeed = new AsyncChangeFeed(r.table("famousNinjas").changes(), 
  { r.connection().hostname("").port(0).db("") })
  .onNew { type, newDoc, oldDoc -> map.replace(newDoc["id"], newDoc) }
  .onRemove { map.remove(it["id"] }
  
def changeFeedThread = changeFeed.startFeed()

changeFeedThread.stopFeed()
```

The `AsyncChangeFeed` takes as parameters a `ReqlExpr` where `.changes()` will be applied and an argumentless closure returning a new connection to the RethinkDB Server.

### Type Handlers

The `AsyncChangeFeed` lets you specify handlers for every event type passed through the handler.

```groovy
def changeFeed = new AsyncChangeFeed(r.table("famousNinjas").changes(), 
  { r.connection().hostname("").port(0).db("") })

// new documents
changeFeed.onAdd { newDoc, oldDoc -> }

// changed documents
changeFeed.onChange { newDoc, oldDoc -> }

// initial documents
changeFeed.onInitial { newDoc, ignored -> }

// removed documents
changeFeed.onRemove { removedDoc -> }

// state changes
changeFeed.onStateChange { state -> }
```

### Additional Handlers

Beside the event types you can also listen to *update* and *new* events.

```groovy
// for initial, add, change and remove
changeFeed.onUpdate { type, newDoc, oldDoc -> }

// for initial and add
changeFeed.onNew { new -> }
```

### Concurrency

When starting the feed with `startFeed()` an instance of `ChangeFeedThread` ist returned. To properly stop this freed, close the cursor and connection to rethinkdb, you should call `stopFeed` on it.

```groovy
def changeFeedThread = changeFeed.startFeed()
changeFeedThread.stopFeed()
```

You can still add new handlers on the original `AsyncChangeFeed` instance. You can even start a new thread.

### Feed Initialisation

`AsyncChangeFeed` will automatically tell RethinkDB to initialize and squash the feed. If you don't want this you can add the optional parameters:

```groovy
// first flag includeInitial, second squash
def changeFeed = new AsyncChangeFeed(changes, connFac, false, false)
```

If you want to await until initialization is finished you can call `awaitReady(timeout)` on the thread. It takes a millisecond timeout as parameter (5 seconds by default).

```groovy
changeFeedThread.awaitReady(1000)
```

If the timeout is overstepped a `ChangeFeedException` will be thrown (with a `TimeoutException` as cause).
