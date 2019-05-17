/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor;

import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.value.error.ErrorRecord;
import io.zeebe.protocol.intent.ErrorIntent;
import io.zeebe.util.exception.RecoverableException;
import io.zeebe.util.retry.AbortableRetryStrategy;
import io.zeebe.util.retry.RecoverableRetryStrategy;
import io.zeebe.util.retry.RetryStrategy;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import java.time.Duration;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;

/**
 * Represents the processing state machine, which is executed on normal processing.
 *
 * <pre>
 *
 * +-----------------+             +--------------------+
 * |                 |             |                    |      exception
 * | readNextEvent() |------------>|   processEvent()   |------------------+
 * |                 |             |                    |                  v
 * +-----------------+             +--------------------+            +---------------+
 *           ^                             |                         |               |------+
 *           |                             |         +-------------->|   onError()   |      | exception
 *           |                             |         |  exception    |               |<-----+
 *           |                     +-------v-------------+           +---------------+
 *           |                     |                     |                 |
 *           |                     |    writeEvent()     |                 |
 *           |                     |                     |<----------------+
 * +----------------------+        +---------------------+
 * |                      |                 |
 * | executeSideEffects() |                 v
 * |                      |       +----------------------+
 * +----------------------+       |                      |
 *           ^                    |     updateState()    |
 *           +--------------------|                      |
 *                                +----------------------+
 *                                       ^      |
 *                                       |      | exception
 *                                       |      |
 *                                    +---------v----+
 *                                    |              |
 *                                    |   onError()  |
 *                                    |              |
 *                                    +--------------+
 *                                       ^     |
 *                                       |     |  exception
 *                                       +-----+
 *
 * </pre>
 */
public final class ProcessingStateMachine {

  private static final Logger LOG = Loggers.PROCESSOR_LOGGER;

  public static final String ERROR_MESSAGE_WRITE_EVENT_ABORTED =
      "Expected to write one or more follow up events for event '{}' without errors, but exception was thrown.";
  private static final String ERROR_MESSAGE_ROLLBACK_ABORTED =
      "Expected to roll back the current transaction for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED =
      "Expected to execute side effects for event '{}' successfully, but exception was thrown.";
  private static final String ERROR_MESSAGE_UPDATE_STATE_FAILED =
      "Expected to successfully update state for event '{}' with processor '{}', but caught an exception. Retry.";
  private static final String ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT =
      "Expected to find event processor for event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT =
      "Expected to successfully process event '{}' with processor '{}', but caught an exception. Skip this event.";
  private static final String ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING =
      "Expected to process event '{}' successfully on stream processor '{}', but caught recoverable exception. Retry processing.";
  private static final String PROCESSING_ERROR_MESSAGE =
      "Expected to process event '%s' without errors, but exception occurred with message '%s' .";

  private static final String LOG_ERROR_EVENT_COMMITTED =
      "Error event was committed, we continue with processing.";
  private static final String LOG_ERROR_EVENT_WRITTEN =
      "Error record was written at {}, we will continue with processing if event was committed. Current commit position is {}.";

  private static final Duration PROCESSING_RETRY_DELAY = Duration.ofMillis(250);

  private final ActorControl actor;
  private final int producerId;
  private final String streamProcessorName;
  private final StreamProcessorMetrics metrics;
  private final EventFilter eventFilter;
  private final LogStream logStream;
  private final LogStreamReader logStreamReader;
  private final TypedStreamWriter logStreamWriter;

  private final DbContext dbContext;
  private final RetryStrategy writeRetryStrategy;
  private final RetryStrategy sideEffectsRetryStrategy;
  private final RetryStrategy updateStateRetryStrategy;

  private final BooleanSupplier shouldProcessNext;
  private final BooleanSupplier abortCondition;

  protected final ZeebeState zeebeState;

  private final ErrorRecord errorRecord = new ErrorRecord();
  protected final RecordMetadata metadata = new RecordMetadata();
  private final Map<ValueType, UnpackedObject> eventCache;
  private final RecordProcessorMap recordProcessorMap;

  private final TypedEventImpl typedEvent = new TypedEventImpl();
  protected final TypedResponseWriterImpl responseWriter;
  private SideEffectProducer sideEffectProducer;

  public ProcessingStateMachine(
      ProcessingContext context,
      StreamProcessorMetrics metrics,
      BooleanSupplier shouldProcessNext) {

    this.actor = context.getActor();
    this.producerId = context.getProducerId();
    this.streamProcessorName = context.getStreamProcessorName();
    this.eventFilter = context.getEventFilter();
    this.recordProcessorMap = context.getRecordProcessorMap();
    this.eventCache = context.getEventCache();
    this.logStreamReader = context.getLogStreamReader();
    this.logStreamWriter = context.getLogStreamWriter();
    this.logStream = context.getLogStream();
    this.zeebeState = context.getZeebeState();
    this.dbContext = context.getDbContext();
    this.abortCondition = context.getAbortCondition();

    this.metrics = metrics;
    this.writeRetryStrategy = new AbortableRetryStrategy(actor);
    this.sideEffectsRetryStrategy = new AbortableRetryStrategy(actor);
    this.updateStateRetryStrategy = new RecoverableRetryStrategy(actor);
    this.shouldProcessNext = shouldProcessNext;

    this.responseWriter =
        new TypedResponseWriterImpl(context.getCommandResponseWriter(), logStream.getPartitionId());
  }

  // current iteration
  private LoggedEvent currentEvent;
  private TypedRecordProcessor<?> currentProcessor;
  private ZeebeDbTransaction zeebeDbTransaction;

  private long eventPosition = -1L;
  private long lastSuccessfulProcessedEventPosition = -1L;
  private long lastWrittenEventPosition = -1L;

  private boolean onErrorHandling;
  private long errorRecordPosition = -1;

  private void skipRecord() {
    actor.submit(this::readNextEvent);
    metrics.incrementEventsSkippedCount();
  }

  void readNextEvent() {
    if (shouldProcessNext.getAsBoolean()
        && logStreamReader.hasNext()
        && currentProcessor == null
        && logStream.getCommitPosition() >= errorRecordPosition) {

      if (onErrorHandling) {
        LOG.info(LOG_ERROR_EVENT_COMMITTED);
        onErrorHandling = false;
      }

      currentEvent = logStreamReader.next();

      if (eventFilter == null || eventFilter.applies(currentEvent)) {
        processEvent(currentEvent);
      } else {
        skipRecord();
      }
    }
  }

  private void resetOutput(long sourceRecordPosition) {
    responseWriter.reset();
    logStreamWriter.reset();
    logStreamWriter.configureSourceContext(producerId, sourceRecordPosition);
  }

  public void setSideEffectProducer(final SideEffectProducer sideEffectProducer) {
    this.sideEffectProducer = sideEffectProducer;
  }

  private void processEvent(final LoggedEvent event) {

    // choose next processor
    try {
      metadata.reset();
      event.readMetadata(metadata);

      currentProcessor =
          recordProcessorMap.get(
              metadata.getRecordType(), metadata.getValueType(), metadata.getIntent().value());
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_ON_EVENT_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      skipRecord();
      return;
    }

    // if no ones want to process it skip this event
    if (currentProcessor == null) {
      skipRecord();
      return;
    }

    // processing
    try {
      final UnpackedObject value = eventCache.get(metadata.getValueType());
      value.reset();
      event.readValue(value);
      typedEvent.wrap(event, metadata, value);

      zeebeDbTransaction = dbContext.getCurrentTransaction();
      zeebeDbTransaction.run(
          () -> {
            // processing
            final long position = event.getPosition();
            resetOutput(position);

            // default side effect is responses; can be changed by processor
            sideEffectProducer = responseWriter;
            final boolean isNotOnBlacklist = !zeebeState.isOnBlacklist(typedEvent);
            if (isNotOnBlacklist) {
              currentProcessor.processRecord(
                  position,
                  typedEvent,
                  responseWriter,
                  logStreamWriter,
                  this::setSideEffectProducer);
            }

            zeebeState.markAsProcessed(position);
          });
      metrics.incrementEventsProcessedCount();
      writeEvent();
    } catch (final RecoverableException recoverableException) {
      // recoverable
      LOG.error(
          ERROR_MESSAGE_PROCESSING_FAILED_RETRY_PROCESSING,
          event,
          streamProcessorName,
          recoverableException);
      actor.runDelayed(PROCESSING_RETRY_DELAY, () -> processEvent(currentEvent));
    } catch (final Exception e) {
      LOG.error(ERROR_MESSAGE_PROCESSING_FAILED_SKIP_EVENT, event, streamProcessorName, e);
      onError(e, this::writeEvent);
    }
  }

  private void writeRejectionOnCommand(Throwable exception) {
    final String errorMessage =
        String.format(PROCESSING_ERROR_MESSAGE, typedEvent, exception.getMessage());
    LOG.error(errorMessage, exception);

    if (typedEvent.getMetadata().getRecordType() == RecordType.COMMAND) {
      sendCommandRejectionOnException(errorMessage);
      writeCommandRejectionOnException(errorMessage);
    }
  }

  private void writeCommandRejectionOnException(String errorMessage) {
    logStreamWriter.appendRejection(typedEvent, RejectionType.PROCESSING_ERROR, errorMessage);
  }

  private void sendCommandRejectionOnException(String errorMessage) {
    responseWriter.writeRejectionOnCommand(
        typedEvent, RejectionType.PROCESSING_ERROR, errorMessage);
  }

  private void onError(Throwable processingException, Runnable nextStep) {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.rollback();
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_ROLLBACK_ABORTED, currentEvent, throwable);
          }
          try {
            zeebeDbTransaction = dbContext.getCurrentTransaction();
            zeebeDbTransaction.run(
                () -> {

                  // old on error
                  final long position = typedEvent.getPosition();
                  resetOutput(position);

                  writeRejectionOnCommand(processingException);
                  errorRecord.initErrorRecord(processingException, position);

                  zeebeState.tryToBlacklist(typedEvent, errorRecord::setWorkflowInstanceKey);

                  logStreamWriter.appendFollowUpEvent(
                      typedEvent.getKey(), ErrorIntent.CREATED, errorRecord);
                });
            onErrorHandling = true;
            nextStep.run();
          } catch (Exception ex) {
            onError(ex, nextStep);
          }
        });
  }

  private void writeEvent() {
    final ActorFuture<Boolean> retryFuture =
        writeRetryStrategy.runWithRetry(
            () -> {
              eventPosition = logStreamWriter.flush();
              return eventPosition >= 0;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, t) -> {
          if (t != null) {
            LOG.error(ERROR_MESSAGE_WRITE_EVENT_ABORTED, currentEvent, t);
            onError(t, this::writeEvent);
          } else {
            metrics.incrementEventsWrittenCount();
            updateState();
          }
        });
  }

  private void updateState() {
    final ActorFuture<Boolean> retryFuture =
        updateStateRetryStrategy.runWithRetry(
            () -> {
              zeebeDbTransaction.commit();

              // needs to be directly after commit
              // so no other ActorJob can interfere between commit and update the positions
              if (onErrorHandling) {
                errorRecordPosition = eventPosition;
                LOG.info(
                    LOG_ERROR_EVENT_WRITTEN, errorRecordPosition, logStream.getCommitPosition());
              }
              lastSuccessfulProcessedEventPosition = currentEvent.getPosition();
              lastWrittenEventPosition = eventPosition;
              return true;
            },
            abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(
                ERROR_MESSAGE_UPDATE_STATE_FAILED, currentEvent, streamProcessorName, throwable);
            onError(throwable, this::updateState);
          } else {
            executeSideEffects();
          }
        });
  }

  private void executeSideEffects() {
    final ActorFuture<Boolean> retryFuture =
        sideEffectsRetryStrategy.runWithRetry(sideEffectProducer::flush, abortCondition);

    actor.runOnCompletion(
        retryFuture,
        (bool, throwable) -> {
          if (throwable != null) {
            LOG.error(ERROR_MESSAGE_EXECUTE_SIDE_EFFECT_ABORTED, currentEvent, throwable);
          }

          // continue with next event
          currentProcessor = null;
          actor.submit(this::readNextEvent);
        });
  }

  public long getLastSuccessfulProcessedEventPosition() {
    return lastSuccessfulProcessedEventPosition;
  }

  public long getLastWrittenEventPosition() {
    return lastWrittenEventPosition;
  }

  public ActorFuture<Long> getLastWrittenPositionAsync() {
    return actor.call(this::getLastWrittenEventPosition);
  }

  public ActorFuture<Long> getLastProcessedPositionAsync() {
    return actor.call(this::getLastSuccessfulProcessedEventPosition);
  }
}
