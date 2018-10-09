package org.github.kpull.transcend.eventsourcing.core


import com.mongodb.MongoClient
import de.flapdoodle.embed.mongo.MongodStarter
import de.flapdoodle.embed.mongo.config.IMongodConfig
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.process.runtime.Network
import org.bson.Document
import org.joda.time.DateTime
import spock.lang.Specification

class MongoDatastoreUpcastersTest extends Specification {

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
        return new MongoDatastore<CurrencyAccount>(mongoClient, "Test", "Coll", new CurrencyAccountSerde(), 1, [new CurrencyAccountUpcaster()] as JsonUpcaster[])
    }

    def "An old version aggregate"(MongoClient mongoClient, Account aggregate) {
        def json = new AccountSerde().serialize(aggregate)
        mongoClient.getDatabase("Test").getCollection("Coll")
                .insertOne(Document.parse(json.toString()))
    }

    def "Store new aggregate"() {
        given:
        def datastore = "A Mongo Datastore"(mongo)
        "An old version aggregate"(mongo, new Account(5L, 0L, 0, DateTime.parse("2018-10-10T10:00:00Z"), new BigDecimal("20.42")))

        when: "I load an old-version aggregate"
        def aggregate = datastore.load(5L)

        then:
        aggregate == new CurrencyAccount(5L, 0L, 1, DateTime.parse("2018-10-10T10:00:00Z"), "EUR", new BigDecimal("20.42"))
    }

}
