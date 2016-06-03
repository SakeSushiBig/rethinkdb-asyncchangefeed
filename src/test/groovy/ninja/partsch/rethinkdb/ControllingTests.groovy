package ninja.partsch.rethinkdb

import com.rethinkdb.RethinkDB
import ninja.partsch.AsyncChangeFeed
import ninja.partsch.exceptions.ChangeFeedException
import spock.lang.Specification

import static com.rethinkdb.RethinkDB.r


class ControllingTests extends RethinkDBNinjaTest {

    def "await ready state"() {
        setup:
        int initials = 0
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
            .onInitial {newDoc, oldDoc -> initials++}
            .startFeed()
        when:
        changeFeed.awaitReady(1000)
        then:
        initials == 5
        cleanup:
        changeFeed.stopFeed()
    }

    def "timeout when awaiting ready state"() {
        setup:
        def changeFeed = new AsyncChangeFeed(r.table("famousNinjas"), connectionFactory)
            .onInitial {newDoc, oldDoc -> sleep(100)} // sleep on every initial to ensure it takes long
            .startFeed()
        when:
        changeFeed.awaitReady(0)
        then:
        thrown(ChangeFeedException)
        cleanup:
        changeFeed.stopFeed()
    }

}