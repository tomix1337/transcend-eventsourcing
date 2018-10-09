package org.github.kpull.transcend.eventsourcing.core


import com.mongodb.MongoClient
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.IMongodConfig
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import org.joda.time.DateTime
import spock.lang.Specification

class MongoDatastoreTest extends Specification {

    private static mongod
    private static mongo

    def setupSpec() {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        String bindIp = "localhost";
        int port = 12345;
        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(bindIp, port, Network.localhostIsIPv6()))
                .build();

        def mongodExecutable = starter.prepare(mongodConfig);
        mongod = mongodExecutable.start();

        mongo = new MongoClient(bindIp, port);
    }

    def cleanupSpec() {
        if (mongod != null) {
            mongod.stop()
        }
    }

    def "A Mongo Datastore"(MongoClient mongoClient) {
        return new MongoDatastore<Account>(mongoClient, "Test", "Coll", new AccountSerde(), 0, new JsonUpcaster[0])
    }

    def "Store new aggregate"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        def success = datastore.store(-1L, new Account(5L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))

        then:
        success
    }

    def "Store existing aggregate"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        datastore.store(-1L, new Account(8L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))
        def success = datastore.store(0L, new Account(8L, 1L, 0, DateTime.parse("2018-10-10T11:00:00Z"), new BigDecimal("20.52")))

        then:
        success
    }

    def "Store aggregate with wrong version"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        def success = datastore.store(-1L, new Account(5L, 0L, 1, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))

        then:
        def exception = thrown(IllegalArgumentException)
        exception.message.startsWith("Cannot store an arbitrary version of the aggregate")
    }

    def "Store aggregate with wrong sequence when it doesn't exist yet"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        def success = datastore.store(1, new Account(6L, 2L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))

        then:
        !success
    }

    def "Store aggregate with wrong sequence when it exists"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        datastore.store(-1L, new Account(7L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))
        def success = datastore.store(1, new Account(7L, 2L, 0, DateTime.parse("2018-10-10T11:00:00Z"), new BigDecimal("20.52")))

        then:
        !success
    }

    def "Store aggregate with lower sequence"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "I store an aggregate"
        datastore.store(-1L, new Account(9L, 2L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))
        def success = datastore.store(2, new Account(9L, 1L, 0, DateTime.parse("2018-10-10T11:00:00Z"), new BigDecimal("20.52")))

        then:
        def exception = thrown(IllegalArgumentException)
        exception.message.startsWith("'aggregate.sequence' must be larger than the 'expectedSequence'")
    }

    def "Load existing aggregate"() {
        given: "A datastore with an aggregate"
        def datastore = "A Mongo Datastore"(mongo)
        datastore.store(-1L, new Account(10L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))

        when: "I load the aggregate"
        def aggregate = datastore.load(10L)

        then:
        aggregate == new Account(10L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42"))
    }

    def "Load inexisting aggregate"() {
        given: "A datastore with an aggregate"
        def datastore = "A Mongo Datastore"(mongo)

        when: "I load the aggregate"
        def aggregate = datastore.load(11L)

        then:
        aggregate == null
    }

    def "Max Version"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)

        when: "We ask for the max version"
        def maxVersion = datastore.maxVersion

        then: "It should match the one supplied during construction"
        maxVersion == 0
    }

}
