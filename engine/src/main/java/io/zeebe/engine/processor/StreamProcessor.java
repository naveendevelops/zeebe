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

import static io.zeebe.engine.processor.TypedEventRegistry.EVENT_REGISTRY;

import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.logstreams.impl.Loggers;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.LangUtil;
import io.zeebe.util.ReflectUtil;
import io.zeebe.util.metrics.MetricsManager;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorCondition;
import io.zeebe.util.sched.ActorScheduler;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

public class StreamProcessor extends Actor implements Service<StreamProcessor> {
  private static final String ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED =
      "Expected to find event with the snapshot position %s in log stream, but nothing was found. Failed to recover '%s'.";
  private static final Logger LOG = Loggers.LOGSTREAMS_LOGGER;

  private final ActorScheduler actorScheduler;
  private final AtomicBoolean isOpened = new AtomicBoolean(false);
  private final List<StreamProcessorLifecycleAware> lifecycleAwareListeners;

  // log stream
  private final LogStream logStream;
  private final int partitionId;
  private ActorCondition onCommitPositionUpdatedCondition;

  // snapshotting
  private final SnapshotController snapshotController;
  private AsyncSnapshotDirector asyncSnapshotDirector;
  private final boolean deleteDataOnSnapshot;
  private final int maxSnapshots;
  private final Duration snapshotPeriod;

  private long snapshotPosition = -1L;

  // processing
  private final ProcessingContext processingContext;
  private StreamProcessorMetrics metrics;
  private final TypedRecordProcessorFactory typedRecordProcessorFactory;
  private final LogStreamReader logStreamReader;
  private ProcessingStateMachine processingStateMachine;

  private Phase phase = Phase.REPROCESSING;

  protected StreamProcessor(final StreamProcessorBuilder context) {
    this.actorScheduler = context.getActorScheduler();
    this.lifecycleAwareListeners = context.getLifecycleListeners();

    this.typedRecordProcessorFactory = context.getTypedRecordProcessorFactory();

    this.snapshotPeriod = context.getSnapshotPeriod();
    this.maxSnapshots = context.getMaxSnapshots();
    this.snapshotController = context.getSnapshotController();
    this.deleteDataOnSnapshot = context.isDeleteDataOnSnapshot();

    final EnumMap<ValueType, UnpackedObject> eventCache = new EnumMap<>(ValueType.class);
    EVENT_REGISTRY.forEach((t, c) -> eventCache.put(t, ReflectUtil.newInstance(c)));

    processingContext =
        context
            .getProcessingContext()
            .eventCache(Collections.unmodifiableMap(eventCache))
            .actor(actor)
            .abortCondition(this::isClosed);
    this.logStreamReader = processingContext.getLogStreamReader();
    this.logStream = processingContext.getLogStream();
    this.partitionId = logStream.getPartitionId();
  }

  public static StreamProcessorBuilder builder(int processorId, String name) {
    return new StreamProcessorBuilder(processorId, name);
  }

  @Override
  public String getName() {
    return processingContext.getStreamProcessorName();
  }

  @Override
  public StreamProcessor get() {
    return this;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    startContext.async(openAsync());
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stopContext.async(closeAsync());
  }

  public ActorFuture<Void> openAsync() {
    if (isOpened.compareAndSet(false, true)) {
      return actorScheduler.submitActor(this, true);
    } else {
      return CompletableActorFuture.completed(null);
    }
  }

  @Override
  protected void onActorStarting() {
    final MetricsManager metricsManager = actorScheduler.getMetricsManager();
    processingContext.metricsManager(metricsManager);
    metrics = new StreamProcessorMetrics(metricsManager, getName(), Integer.toString(partitionId));
  }

  @Override
  protected void onActorStarted() {
    try {
      LOG.info("Recovering state of partition {} from snapshot", partitionId);
      snapshotPosition = recoverFromSnapshot();

      initProcessors();

      lifecycleAwareListeners.forEach(l -> l.onOpen(processingContext));
    } catch (final Throwable e) {
      onFailure();
      LangUtil.rethrowUnchecked(e);
    }

    try {
      processingStateMachine =
          new ProcessingStateMachine(processingContext, metrics, () -> isOpened());

      final ReProcessingStateMachine reProcessingStateMachine =
          new ReProcessingStateMachine(processingContext);

      final ActorFuture<Void> recoverFuture =
          reProcessingStateMachine.startRecover(snapshotPosition);

      actor.runOnCompletion(
          recoverFuture,
          (v, throwable) -> {
            if (throwable != null) {
              LOG.error("Unexpected error on recovery happens.", throwable);
              onFailure();
            } else {
              onRecovered();
            }
          });
    } catch (final RuntimeException e) {
      onFailure();
      throw e;
    }
  }

  private void initProcessors() {
    final TypedRecordProcessors typedRecordProcessors =
        typedRecordProcessorFactory.createProcessors(processingContext);

    lifecycleAwareListeners.addAll(typedRecordProcessors.getLifecycleListeners());
    final RecordProcessorMap recordProcessorMap = typedRecordProcessors.getRecordProcessorMap();
    recordProcessorMap.values().forEachRemaining(this.lifecycleAwareListeners::add);

    processingContext.recordProcessorMap(recordProcessorMap);
  }

  private long recoverFromSnapshot() throws Exception {
    final ZeebeState zeebeState = recoverState();
    final long snapshotPosition = zeebeState.getLastSuccessfuProcessedRecordPosition();

    final boolean failedToRecoverReader = !logStreamReader.seekToNextEvent(snapshotPosition);
    if (failedToRecoverReader) {
      throw new IllegalStateException(
          String.format(ERROR_MESSAGE_RECOVER_FROM_SNAPSHOT_FAILED, snapshotPosition, getName()));
    }

    LOG.info(
        "Recovered state of partition {} from snapshot at position {}",
        partitionId,
        snapshotPosition);
    return snapshotPosition;
  }

  private ZeebeState recoverState() throws Exception {
    snapshotController.recover();
    final ZeebeDb zeebeDb = snapshotController.openDb();
    final DbContext dbContext = zeebeDb.createContext();
    final ZeebeState zeebeState = new ZeebeState(partitionId, zeebeDb, dbContext);

    processingContext.dbContext(dbContext);
    processingContext.zeebeState(zeebeState);

    return zeebeState;
  }

  private void onRecovered() {
    phase = Phase.PROCESSING;

    asyncSnapshotDirector =
        new AsyncSnapshotDirector(
            getName(),
            snapshotPeriod,
            processingStateMachine::getLastProcessedPositionAsync,
            processingStateMachine::getLastWrittenPositionAsync,
            snapshotController,
            logStream::registerOnCommitPositionUpdatedCondition,
            logStream::removeOnCommitPositionUpdatedCondition,
            logStream::getCommitPosition,
            metrics.getSnapshotMetrics(),
            maxSnapshots,
            deleteDataOnSnapshot ? logStream::delete : pos -> {});

    actorScheduler.submitActor(asyncSnapshotDirector);

    onCommitPositionUpdatedCondition =
        actor.onCondition(
            getName() + "-on-commit-position-updated", processingStateMachine::readNextEvent);
    logStream.registerOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);

    // start reading
    lifecycleAwareListeners.forEach(l -> l.onRecovered(processingContext));
    actor.submit(processingStateMachine::readNextEvent);
  }

  public ActorFuture<Void> closeAsync() {
    if (isOpened.compareAndSet(true, false)) {
      return actor.close();
    } else {
      return CompletableActorFuture.completed(null);
    }
  }

  @Override
  protected void onActorCloseRequested() {
    if (!isFailed()) {
      lifecycleAwareListeners.forEach(l -> l.onClose());
    }
  }

  @Override
  protected void onActorClosing() {
    metrics.close();

    if (!isFailed()) {
      actor.run(
          () -> {
            if (asyncSnapshotDirector != null) {
              actor.runOnCompletionBlockingCurrentPhase(
                  asyncSnapshotDirector.enforceSnapshotCreation(
                      processingStateMachine.getLastWrittenEventPosition(),
                      processingStateMachine.getLastSuccessfulProcessedEventPosition()),
                  (v, ex) -> {
                    try {
                      asyncSnapshotDirector.close();
                      snapshotController.close();
                    } catch (Exception e) {
                      LOG.error("Error on closing snapshotController.", e);
                    }
                  });
            } else {
              try {
                snapshotController.close();
              } catch (Exception e) {
                LOG.error("Error on closing snapshotController.", e);
              }
            }
          });
    }

    processingContext.getLogStreamReader().close();

    if (onCommitPositionUpdatedCondition != null) {
      logStream.removeOnCommitPositionUpdatedCondition(onCommitPositionUpdatedCondition);
      onCommitPositionUpdatedCondition = null;
    }
  }

  @Override
  protected void onActorClosed() {
    LOG.debug("Closed stream processor controller {}.", getName());
  }

  private void onFailure() {
    phase = Phase.FAILED;

    isOpened.set(false);

    actor.close();
  }

  public boolean isOpened() {
    return isOpened.get();
  }

  public boolean isClosed() {
    return !isOpened.get();
  }

  public boolean isFailed() {
    return phase == Phase.FAILED;
  }

  private enum Phase {
    REPROCESSING,
    PROCESSING,
    FAILED
  }
}
