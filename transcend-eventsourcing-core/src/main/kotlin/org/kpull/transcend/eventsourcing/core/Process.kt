package org.kpull.transcend.eventsourcing.core

import com.fasterxml.jackson.databind.JsonNode
import org.joda.time.DateTime

interface Aggregate {
    val identifier: Long
    val sequence: Long
    val version: Int
    val updatedOn: DateTime
}

interface Command {
    val intent: Long
    val timestamp: DateTime
    val identifier: Long
}

interface Result {
    val success: Boolean
}

data class FailureResult(
        val intent: Long?,
        val code: String,
        val message: String) : Result {
    override val success = false
}

data class SuccessfulResult<T : Aggregate>(
        val intent: Long,
        val aggregate: T) : Result {
    override val success = true
}

interface Handler<T : Aggregate> {
    fun handle(aggregate: T?, command: Command): T
}

interface Datastore<T : Aggregate> {
    companion object {
        val NO_PREVIOUS_SEQUENCE = -1L
    }

    val maxVersion: Int

    fun store(expectedSequence: Long, aggregate: T): Boolean
    fun load(identifier: Long): T?
}

interface Processor<T : Aggregate> {
    fun submit(command: Command, onSuccess: (SuccessfulResult<T>) -> Unit, onError: (FailureResult) -> Unit)
}

interface JsonProcessorSerde<T : Aggregate> {
    fun serializeSuccessfulResult(result: SuccessfulResult<T>): JsonNode
    fun serializeFailureResult(result: FailureResult): JsonNode
    fun deserializeCommand(command: JsonNode): Command
}

interface JsonUpcaster {
    fun upcast(old: JsonNode): JsonNode
}

interface JsonAggregateSerde<T : Aggregate> {
    fun serialize(aggregate: T): JsonNode
    fun deserialize(json: JsonNode): T
}

