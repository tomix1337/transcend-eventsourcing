package org.github.kpull.transcend.eventsourcing.core

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.joda.time.DateTime
import org.kpull.transcend.eventsourcing.core.Aggregate
import org.kpull.transcend.eventsourcing.core.JsonAggregateSerde
import java.math.BigDecimal

data class Account(override val identifier: Long,
                   override val sequence: Long,
                   override val version: Int,
                   override val updatedOn: DateTime,
                   val balance: BigDecimal) : Aggregate

class AccountSerde : JsonAggregateSerde<Account> {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    override fun serialize(aggregate: Account): JsonNode {
        val json = objectMapper.createObjectNode()
        json.put("identifier", aggregate.identifier)
                .put("sequence", aggregate.sequence)
                .put("version", aggregate.version)
                .put("updatedOn", aggregate.updatedOn.toString())
                .put("balance", aggregate.balance)
        return json
    }

    override fun deserialize(json: JsonNode) = Account(json["identifier"].longValue(),
            json["sequence"].longValue(),
            json["version"].intValue(),
            DateTime.parse(json["updatedOn"].textValue()),
            json["balance"].decimalValue())

}