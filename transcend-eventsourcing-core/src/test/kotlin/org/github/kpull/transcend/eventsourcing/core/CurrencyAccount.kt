package org.github.kpull.transcend.eventsourcing.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.joda.time.DateTime
import org.kpull.transcend.eventsourcing.core.Aggregate
import org.kpull.transcend.eventsourcing.core.JsonAggregateSerde
import org.kpull.transcend.eventsourcing.core.JsonUpcaster
import java.math.BigDecimal

data class CurrencyAccount(override val identifier: Long,
                           override val sequence: Long,
                           override val version: Int,
                           override val updatedOn: DateTime,
                           val currency: String,
                           val balance: BigDecimal) : Aggregate

class CurrencyAccountUpcaster() : JsonUpcaster {
    override fun upcast(old: JsonNode): JsonNode {
        val upcasted = old.deepCopy<ObjectNode>()
        return upcasted.put("version", 1).put("currency", "EUR")
    }
}

class CurrencyAccountSerde : JsonAggregateSerde<CurrencyAccount> {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    override fun serialize(aggregate: CurrencyAccount): JsonNode {
        val json = objectMapper.createObjectNode()
        json.put("identifier", aggregate.identifier)
                .put("sequence", aggregate.sequence)
                .put("version", aggregate.version)
                .put("updatedOn", aggregate.updatedOn.toString())
                .put("currency", aggregate.currency)
                .put("balance", aggregate.balance)
        return json
    }

    override fun deserialize(json: JsonNode) = CurrencyAccount(json["identifier"].longValue(),
            json["sequence"].longValue(),
            json["version"].intValue(),
            DateTime.parse(json["updatedOn"].textValue()),
            json["currency"].textValue(),
            json["balance"].decimalValue())

}