package org.kpull.transcend.eventsourcing.core

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class AsyncProcessor<T : Aggregate>(private val datastore: Datastore<T>,
                                    private val handler: Handler<T>,
                                    maximumProcessors: Int = 10,
                                    maximumQueuedCommands: Int = 1000) : Processor<T> {

    private val executor = ThreadPoolExecutor(1,
            maximumProcessors,
            30000L, TimeUnit.MILLISECONDS,
            ArrayBlockingQueue<Runnable>(maximumQueuedCommands, false))

    override fun submit(command: Command, onSuccess: (SuccessfulResult<T>) -> Unit, onError: (FailureResult) -> Unit) {
        var nextAggregate: T
        executor.submit {
            try {
                do {
                    val previousAggregate = datastore.load(command.identifier)
                    nextAggregate = handler.handle(previousAggregate, command)
                } while (!datastore.store(previousAggregate?.sequence ?: Datastore.NO_PREVIOUS_SEQUENCE, nextAggregate))
                val result = SuccessfulResult(command.intent, nextAggregate)
                onSuccess(result)
            } catch (e: RuntimeException) {
                onError(FailureResult(command.intent, "UNKNOWN", "An unknown error has occurred"))
            }
        }
    }

}