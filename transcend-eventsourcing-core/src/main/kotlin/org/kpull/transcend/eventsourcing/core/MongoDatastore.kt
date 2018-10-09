package org.kpull.transcend.eventsourcing.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.mongodb.client.MongoClient
import org.bson.Document

class MongoDatastore<T : Aggregate>(mongo: MongoClient,
                                    database: String,
                                    collection: String,
                                    private val serde: JsonAggregateSerde<T>,
                                    override val maxVersion: Int,
                                    private val upcasters: Array<JsonUpcaster>) : Datastore<T> {

    private val objectMapper: ObjectMapper

    private val collection = mongo.getDatabase(database).getCollection(collection)

    init {
        if (maxVersion < 0) {
            throw IllegalArgumentException("'maxVersion' must be at least 0")
        }
        if (upcasters.size != maxVersion) {
            throw IllegalArgumentException("There must be exactly $maxVersion upcasters configured")
        }
        objectMapper = ObjectMapper().registerModule(KotlinModule())
    }

    override fun store(expectedSequence: Long, aggregate: T): Boolean {
        if (expectedSequence < -1) {
            throw IllegalArgumentException("'expectedSequence' must be at least -1")
        }
        if (aggregate.version != maxVersion) {
            throw IllegalArgumentException("Cannot store an arbitrary version of the aggregate. Make sure 'aggregate.version' (${aggregate.version}) " +
                    "matches this store's version ($maxVersion).")
        }
        if (aggregate.sequence <= expectedSequence) {
            throw IllegalArgumentException("'aggregate.sequence' must be larger than the 'expectedSequence'")
        }

        val serializedJson = serde.serialize(aggregate)
        val identifierJson = serializedJson.get("identifier")
        if (identifierJson == null || !identifierJson.isIntegralNumber) {
            throw IllegalStateException("Serializer did not produce an 'identifier' property in the document or it is not a number. Please review serde configuration.")
        }
        val sequenceJson = serializedJson.get("sequence")
        if (sequenceJson == null || !sequenceJson.isIntegralNumber) {
            throw IllegalStateException("Serializer did not produce a 'sequence' property in the document or it is not a number. Please review serde configuration.")
        }

        val serializedBson = Document.parse(objectMapper.writeValueAsString(serializedJson))

        return if (expectedSequence == Datastore.NO_PREVIOUS_SEQUENCE) {
            compareAndInsert(serializedBson)
        } else {
            compareAndReplace(aggregate.identifier, expectedSequence, serializedBson)
        }
    }

    override fun load(identifier: Long): T? {
        val serializedBson = collection.find(identifierFilter(identifier)).first() ?: return null

        val serializedJson = objectMapper.readTree(serializedBson.toJson())
        val serializedVersion = serializedJson.get("version")
        if (serializedVersion == null || !serializedVersion.isNumber) {
            throw IllegalStateException("Loaded document 'version' field is not present or it is not a number")
        }
        val version = serializedVersion.intValue()
        if (version < 0 || version > maxVersion) {
            throw IllegalStateException("Loaded document 'version' ($version) must be at least 0 and at most $maxVersion")
        }
        val upcastedJson = upcast(serializedJson)

        val aggregate = serde.deserialize(upcastedJson)
        if (aggregate.identifier != identifier) {
            throw IllegalArgumentException("'identifier' of parsed aggregate must be equal to the supplied argument")
        }
        if (aggregate.sequence < -1) {
            throw IllegalArgumentException("'sequence' of parsed aggregate must be at least -1")
        }
        if (aggregate.version != maxVersion) {
            throw IllegalArgumentException("'version' of parsed aggregate must be equal to the store's max version")
        }
        return aggregate
    }

    private fun upcast(serializedJson: JsonNode): JsonNode {
        var lastUpcastedJson = serializedJson
        var counter = 0;

        while (counter <= maxVersion) {
            val serializedVersion = lastUpcastedJson.get("version")
            val version = serializedVersion.intValue()

            if (version > maxVersion) {
                throw IllegalStateException("An upcasted document has a version higher than that allowed. Please review upcasters configuration.")
            }

            if (version == maxVersion) {
                return lastUpcastedJson;
            }

            if (version < maxVersion) {
                val upcasters = upcasters[version]
                lastUpcastedJson = upcasters.upcast(serializedJson)
            }

            counter++;
        }

        throw IllegalStateException("Took too many iterations to upcast to the correct version. Please review upcasters configuration.")
    }

    private fun compareAndInsert(serializedBson: Document): Boolean {
        collection.insertOne(serializedBson)
        return true
    }

    private fun compareAndReplace(identifier: Long, expectedSequence: Long, serializedBson: Document): Boolean =
            collection.findOneAndReplace(
                    identifierAndSequenceFilter(identifier, expectedSequence),
                    serializedBson) != null

    fun identifierFilter(identifier: Long) = Document("identifier", identifier)

    fun identifierAndSequenceFilter(identifier: Long, expectedSequence: Long) = Document(mapOf(
            "identifier" to identifier,
            "sequence" to expectedSequence
    ))

}