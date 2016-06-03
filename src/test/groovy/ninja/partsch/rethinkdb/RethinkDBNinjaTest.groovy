package ninja.partsch.rethinkdb

import com.rethinkdb.net.Connection
import groovy.json.JsonSlurper
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Paths

import static com.rethinkdb.RethinkDB.r


class RethinkDBNinjaTest extends Specification {

    def connectionFactory = {r.connection()
            .hostname("192.168.99.100")
            .port(28015)
            .db("ninjas")
            .connect()}
    @Shared
    Connection dbConn

    def setupSpec() {
        // connect to RethinkDB Server
        dbConn = r.connection()
                .hostname("192.168.99.100")
                .port(28015)
                .connect()
        // create test database
        if(r.dbList().contains("ninjas").run(dbConn))
            r.dbDrop("ninjas")
                    .run(dbConn)
        def result = r.dbCreate("ninjas")
                .run(dbConn)
        if(result["dbs_created"] < 1)
            throw new RuntimeException("could not create test database")
        dbConn.use("ninjas")
        // create test table
        result = r.tableCreate("famousNinjas")
                .run(dbConn)
        if(result["tables_created"] < 1)
            throw new RuntimeException("could not create test table")
    }

    def setup() {
        // load test documents
        def slurper = new JsonSlurper()
        def famousNinjas
        Paths.get(getClass().getResource("/famous-ninjas.json").toURI()).withReader {
            famousNinjas = slurper.parse(it)
        }
        def result = r.table("famousNinjas")
                .insert(famousNinjas)
                .run(dbConn)
        if(result["inserted"] < 5)
            throw new RuntimeException("could not insert all test documents")
    }

    def cleanup() {
        // remove all test documents
        r.table("famousNinjas")
                .delete()
                .run(dbConn)
    }

    def cleanupSpec() {
        // drop database
        r.dbDrop("ninjas")
                .run(dbConn)
        // close connection
        dbConn.close()
    }
}