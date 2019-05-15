/*
 * Zeebe Broker Core
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
package io.zeebe.broker.clustering.base.partitions;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.engine.EngineService;
import io.zeebe.broker.exporter.stream.ExporterStreamProcessorState;
import io.zeebe.broker.logstreams.state.DefaultOnDemandSnapshotReplication;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.db.ZeebeDb;
import io.zeebe.engine.processor.AsyncSnapshotDirector;
import io.zeebe.engine.processor.StreamProcessorService;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.DurationUtil;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;

/** Service representing a partition. */
public class Partition implements Service<Partition> {
  public static final String PARTITION_NAME_FORMAT = "raft-atomix-partition-%d";
  public static final Logger LOG = Loggers.CLUSTERING_LOGGER;
  private final BrokerCfg brokerCfg;
  private final ClusterCommunicationService communicationService;

  private SnapshotReplication stateReplication;
  private DefaultOnDemandSnapshotReplication snapshotRequestServer;
  private ExecutorService executor;

  public static String getPartitionName(final int partitionId) {
    return String.format(PARTITION_NAME_FORMAT, partitionId);
  }

  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<StateSnapshotController> snapshotControllerInjector = new Injector<>();
  private final Injector<StreamProcessorService> streamProcessorServiceInjector = new Injector<>();

  private final int partitionId;
  private final RaftState state;

  private LogStream logStream;
  private StateSnapshotController snapshotController;

  public Partition(
      BrokerCfg brokerCfg,
      ClusterCommunicationService communicationService,
      final int partitionId,
      final RaftState state) {
    this.brokerCfg = brokerCfg;
    this.partitionId = partitionId;
    this.state = state;
    this.communicationService = communicationService;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    final String streamProcessorName = EngineService.PROCESSOR_NAME;
    logStream = logStreamInjector.getValue();

    // TODO: should this be closed here or in its service (analyze dependencies)
    this.snapshotController = snapshotControllerInjector.getValue();

    if (state == RaftState.FOLLOWER) {
      logStream.setExporterPositionSupplier(this::getLowestReplicatedExportedPosition);

      this.snapshotController.consumeReplicatedSnapshots(logStream::delete);
    } else {
      createSnapshotDirector(startContext);
      executor =
          Executors.newSingleThreadExecutor(
              (r) -> new Thread(r, String.format("snapshot-request-server-%d", partitionId)));
      snapshotRequestServer =
          new DefaultOnDemandSnapshotReplication(
              communicationService, partitionId, streamProcessorName, executor);
      snapshotRequestServer.serve(
          request -> {
            LOG.info("Received snapshot replication request for partition {}", partitionId);
            this.snapshotController.replicateLatestSnapshot(Runnable::run);
          });
    }
  }

  private void createSnapshotDirector(final ServiceStartContext startContext) {
    final Duration snapshotPeriod = DurationUtil.parse(brokerCfg.getData().getSnapshotPeriod());
    final int maxSnapshots = brokerCfg.getData().getMaxSnapshots();

    final AsyncSnapshotDirector asyncSnapshotDirector =
        new AsyncSnapshotDirector(
            EngineService.PROCESSOR_NAME,
            streamProcessorServiceInjector.getValue().getController(),
            snapshotController,
            logStream,
            snapshotPeriod,
            maxSnapshots);

    startContext.getScheduler().submitActor(asyncSnapshotDirector);
  }

  private long getLowestReplicatedExportedPosition() {
    try {
      if (snapshotController.getValidSnapshotsCount() > 0) {
        snapshotController.recover();
        final ZeebeDb zeebeDb = snapshotController.openDb();
        final ExporterStreamProcessorState exporterState =
            new ExporterStreamProcessorState(zeebeDb, zeebeDb.createContext());

        final long lowestPosition = exporterState.getLowestPosition();

        LOG.debug(
            "The lowest exported position at follower {} is {}.",
            brokerCfg.getCluster().getNodeId(),
            lowestPosition);
        return lowestPosition;
      } else {
        LOG.info(
            "Follower {} has no exporter snapshot so it can't delete data.",
            brokerCfg.getCluster().getNodeId());
      }
    } catch (Exception e) {
      LOG.error(
          "Unexpected error occurred while obtaining the lowest exported position at a follower.",
          e);
    } finally {
      try {
        snapshotController.close();
      } catch (Exception e) {
        LOG.error("Unexpected error occurred while closing the DB.", e);
      }
    }

    return -1;
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stateReplication.close();
    if (snapshotRequestServer != null) {
      snapshotRequestServer.close();
    }
    if (executor != null) {
      executor.shutdown();
    }
  }

  @Override
  public Partition get() {
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public StateSnapshotController getSnapshotController() {
    return snapshotController;
  }

  public RaftState getState() {
    return state;
  }

  public LogStream getLogStream() {
    return logStream;
  }

  public Injector<LogStream> getLogStreamInjector() {
    return logStreamInjector;
  }

  public Injector<StreamProcessorService> getStreamProcessorServiceInjector() {
    return streamProcessorServiceInjector;
  }

  public Injector<StateSnapshotController> getSnapshotControllerInjector() {
    return snapshotControllerInjector;
  }
}
