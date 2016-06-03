package ninja.partsch.rethinkdb

import ninja.partsch.AsyncChangeFeed
import spock.lang.Stepwise

import static com.rethinkdb.RethinkDB.r

class ReactionTests extends RethinkDBNinjaTest {

    def "react to added documents"() {
        setup:
        def addedElements = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
                            .onAdd({newDoc, oldDoc -> addedElements.add(newDoc)})
                            .startFeed()
        when:
        r.table("famousNinjas")
            .insert(r.array(r.hashMap("id", 1001)
                .with("name", "Tony Dancer")
                .with("century", 20)
                .with("clan", r.hashMap("name", "Dancing Tigers")
                .with("position", "Leader")),
                r.hashMap("id", 1002)
                        .with("name", "Sherlock Holmes")
                        .with("century", 19)
                        .with("clan", r.hashMap("name", "Scottland Yard")
                        .with("position", "Private Investigator"))))
            .run(dbConn)

        then:
        sleep(100) // wait for change feed to react
        addedElements.collect {it["name"]} == ["Tony Dancer", "Sherlock Holmes"]
    }

    def "react to changed doucments"() {
        setup:
        def changedElements = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
                .onChange {newDoc, oldDoc -> changedElements.add(newDoc)}
                .startFeed()
        when:
        r.table("famousNinjas")
            .get(1)
            .update(r.hashMap("name", "Freddy Mercury"))
            .run(dbConn)
        then:
        sleep(100) // wait for change feed to react
        changedElements.collect {it["name"]} == ["Freddy Mercury"]
    }

    def "react on initial"() {
        setup:
        def initialized = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
                .onInitial {newDoc, oldDoc -> initialized.add(newDoc)}
                .startFeed()
        expect:
        sleep(100)
        initialized.collect {it["id"]} .sort() == [1,2,3,4,5]
    }

    def "react on remove"() {
        setup:
        def removed = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
            .onRemove {removed.add(it["id"])}
            .startFeed()
        when:
        r.table("famousNinjas")
            .getAll(1,2,3)
            .delete()
            .run(dbConn)
        then:
        sleep(100)
        removed.sort() == [1,2,3]
    }

    def "react on any update"() {
        setup:
        def updates = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
            .onUpdate {type, newDoc, oldDoc -> updates.add(type)}
            .startFeed()
        when:
        sleep(100) // let feed initialize before inserting
        r.table("famousNinjas")
            .insert(r.hashMap("id", 1012)
                        .with("name", "Bat Man")
                        .with("century", 21)
                        .with("clan", r.hashMap("name", "Justice League")
                                        .with("position", "Member")))
            .run(dbConn)
        r.table("famousNinjas")
            .get(1012)
            .update(r.hashMap("clan", r.hashMap("position", "Co-Leader")))
            .run(dbConn)
        r.table("famousNinjas")
            .get(1)
            .delete()
            .run(dbConn)
        then:
        sleep(200)
        updates == ["initial", "initial", "initial", "initial", "initial", "add", "change", "remove"]
    }

    def "react on any new document"() {
        setup:
        def news = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
                                                .onNew { type, newDoc -> news.add(type)}
                                                .startFeed()
        when:
        sleep(100) // let feed initialize before inserting
        r.table("famousNinjas")
            .insert(r.hashMap("id", 1000)
                        .with("name", "Ned Stark")
                        .with("century", "unknown")
                        .with("clan", r.hashMap("name", "Starks")
                                        .with("position", "Protector of the North")))
                        .run(dbConn)
        then:
        sleep(200)
        news == ["initial", "initial", "initial", "initial", "initial", "add"]
    }

    def "react on states"() {
        setup:
        def states = []
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
                                                .onStateChange {states.add(it)}
                                                .startFeed()
        expect:
        sleep(100)
        states == ["initializing", "ready"]
    }

}