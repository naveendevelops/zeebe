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
package io.zeebe.broker.logstreams.restore;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreServer;
import io.zeebe.distributedlog.restore.RestoreServer.LogReplicationRequestHandler;
import io.zeebe.distributedlog.restore.RestoreServer.RestoreInfoRequestHandler;
import io.zeebe.distributedlog.restore.RestoreServer.SnapshotInfoRequestHandler;
import io.zeebe.distributedlog.restore.RestoreServer.SnapshotRequestHandler;
import io.zeebe.distributedlog.restore.impl.DefaultRestoreInfoRequestHandler;
import io.zeebe.distributedlog.restore.impl.DefaultSnapshotInfoRequestHandler;
import io.zeebe.distributedlog.restore.impl.DefaultSnapshotRequestHandler;
import io.zeebe.distributedlog.restore.log.impl.DefaultLogReplicationRequestHandler;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class BrokerRestoreContext implements AutoCloseable {
  private final int partitionId;
  private final String localMemberId;

  private ClusterCommunicationService communicationService;
  private RestoreServer server;

  public BrokerRestoreContext(
      int partitionId, String localMemberId, ClusterCommunicationService communicationService) {
    this.partitionId = partitionId;
    this.localMemberId = localMemberId;
    this.communicationService = communicationService;
  }

  public void setProcessorSnapshotController(SnapshotController snapshotController) {
    LogstreamConfig.putProcesorSnapshotController(localMemberId, partitionId, snapshotController);
  }

  public void setExporterSnapshotController(SnapshotController snapshotController) {
    LogstreamConfig.putExporterSnapshotController(localMemberId, partitionId, snapshotController);
  }

  @Override
  public void close() {
    stopRestoreServer();
  }

  public CompletableActorFuture<Void> startRestoreServer(
      LogStream logStream,
      SnapshotController processorSnapshotController,
      SnapshotController exporterSnapshotController) {
    final CompletableActorFuture<Void> startedFuture = new CompletableActorFuture<>();
    final LogReplicationRequestHandler logReplicationHandler =
        new DefaultLogReplicationRequestHandler(logStream);
    final RestoreInfoRequestHandler restoreInfoHandler =
        new DefaultRestoreInfoRequestHandler(logStream, processorSnapshotController);
    final SnapshotRequestHandler snapshotRequestHandler =
        new DefaultSnapshotRequestHandler(processorSnapshotController, exporterSnapshotController);
    final SnapshotInfoRequestHandler snapshotInfoRequestHandler =
        new DefaultSnapshotInfoRequestHandler(
            processorSnapshotController, exporterSnapshotController);

    this.server = createServer();
    this.server
        .serve(logReplicationHandler)
        .thenCompose(nothing -> server.serve(restoreInfoHandler))
        .thenCompose(nothing -> server.serve(snapshotRequestHandler))
        .thenCompose(nothing -> server.serve(snapshotInfoRequestHandler))
        .thenAccept(startedFuture::complete);
    return startedFuture;
  }

  public void stopRestoreServer() {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  private BrokerRestoreServer createServer() {
    return new BrokerRestoreServer(
        communicationService,
        BrokerRestoreClientFactory.getLogReplicationTopic(partitionId),
        BrokerRestoreClientFactory.getRestoreInfoTopic(partitionId),
        BrokerRestoreClientFactory.getSnapshotRequestTopic(partitionId),
        BrokerRestoreClientFactory.getSnapshotInfoRequestTopic(partitionId));
  }
}
