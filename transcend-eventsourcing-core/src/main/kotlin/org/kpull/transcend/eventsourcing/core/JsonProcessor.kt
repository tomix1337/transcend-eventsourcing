package org.kpull.transcend.eventsourcing.core

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.log4j.LogManager

interface JsonLogger {

    fun log(content: JsonNode)
    fun logError(content: JsonNode)

}

class Log4jLogger(val loggerName: String) : JsonLogger {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())
    private val logger = LogManager.getLogger(loggerName)

    override fun log(content: JsonNode) {
        logger.info(objectMapper.writeValueAsString(content))
    }

    override fun logError(content: JsonNode) {
        logger.error(objectMapper.writeValueAsString(content))
    }

}

class JsonProcessor<T : Aggregate>(private val delegate: Processor<T>,
                                   private val serde: JsonProcessorSerde<T>,
                                   private val logger: JsonLogger) {

    private val objectMapper = ObjectMapper().registerModule(KotlinModule())

    fun submitAsString(command: String, onSuccess: (String) -> Unit, onError: (String) -> Unit) {
        try {
            val json = objectMapper.readTree(command)
            if (json == null) {
                val serializedFailureResult = serde.serializeFailureResult(FailureResult(null, "INVALID_JSON", "The supplied data is not valid JSON"))
                onError(objectMapper.writeValueAsString(serializedFailureResult))
            } else {
                submitAsJson(json, { success ->
                    onSuccess(objectMapper.writeValueAsString(success))
                }, { error ->
                    onError(objectMapper.writeValueAsString(error))
                })
            }
        } catch (e: JsonParseException) {
            val serializedFailureResult = serde.serializeFailureResult(FailureResult(null, "INVALID_JSON", "The supplied data is not valid JSON"))
            onError(objectMapper.writeValueAsString(serializedFailureResult))
        }
    }

    fun submitAsJson(command: JsonNode, onSuccess: (JsonNode) -> Unit, onError: (JsonNode) -> Unit) {
        val intent = command["intent"]
        if (intent?.isNumber?.not() != false) {
            logAndCallback(logger::logError, onError, serde.serializeFailureResult(FailureResult(null, "INVALID_INTENT", "Serialized 'command' does not have an 'intent' property or it is not a number")))
        } else if (command["timestamp"]?.isTextual?.not() != false) {
            logAndCallback(logger::logError, onError, serde.serializeFailureResult(FailureResult(intent.longValue(), "INVALID_TIMESTAMP", "Serialized 'command' does not have a 'timestamp' property or it is not valid")))
        } else if (command["identifier"]?.isNumber?.not() != false) {
            logAndCallback(logger::logError, onError, serde.serializeFailureResult(FailureResult(intent.longValue(), "INVALID_IDENTIFIER", "Serialized 'command' does not have an 'identifier' property or it is not a number")))
        } else {
            val deserializedCommand = serde.deserializeCommand(command)
            delegate.submit(deserializedCommand, { success ->
                logAndCallback(logger::log, onSuccess, serde.serializeSuccessfulResult(success))
            }, { error ->
                logAndCallback(logger::logError, onError, serde.serializeFailureResult(error))
            })
        }
    }

    private fun logAndCallback(log: (JsonNode) -> Unit, callback: (JsonNode) -> Unit, content: JsonNode) {
        log(content)
        callback(content)
    }

}