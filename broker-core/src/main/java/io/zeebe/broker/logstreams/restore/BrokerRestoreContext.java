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
import io.zeebe.distributedlog.restore.PartitionLeaderElectionController;
import io.zeebe.distributedlog.restore.RestoreInfoServer;
import io.zeebe.distributedlog.restore.RestoreServer;
import io.zeebe.distributedlog.restore.impl.DefaultRestoreInfoServerHandler;
import io.zeebe.distributedlog.restore.log.LogReplicationServer;
import io.zeebe.distributedlog.restore.log.impl.DefaultLogReplicationServerHandler;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class BrokerRestoreContext implements AutoCloseable {
  private final int partitionId;
  private final String localMemberId;
  private final PartitionLeaderElectionController electionController;

  private BrokerRestoreFactory restoreFactory;
  private RestoreServer server;

  public BrokerRestoreContext(
      int partitionId,
      String localMemberId,
      ClusterCommunicationService communicationService,
      PartitionLeaderElectionController electionController) {
    this.partitionId = partitionId;
    this.localMemberId = localMemberId;
    this.electionController = electionController;
    this.restoreFactory = new BrokerRestoreFactory(communicationService, partitionId);
  }

  public void updateLogstreamConfig() {
    LogstreamConfig.putLeaderElectionController(localMemberId, partitionId, electionController);
    LogstreamConfig.putRestoreClientFactory(localMemberId, partitionId, restoreFactory);
  }

  public void clearLogstreamConfig() {
    LogstreamConfig.removeLeaderElectionController(localMemberId, partitionId);
    LogstreamConfig.removeRestoreClientFactory(localMemberId, partitionId);
  }

  @Override
  public void close() {
    stopRestoreServer();
    clearLogstreamConfig();
  }

  public CompletableActorFuture<Void> startRestoreServer(
      LogStream logStream, SnapshotController processorSnapshotController) {
    final CompletableActorFuture<Void> startedFuture = new CompletableActorFuture<>();
    final LogReplicationServer.Handler logReplicationHandler =
        new DefaultLogReplicationServerHandler(logStream);
    final RestoreInfoServer.Handler restoreInfoHandler =
        new DefaultRestoreInfoServerHandler(logStream, processorSnapshotController);

    this.server = restoreFactory.createServer();
    this.server
        .serve(logReplicationHandler)
        .thenCompose(nothing -> server.serve(restoreInfoHandler))
        .thenAccept(startedFuture::complete);
    return startedFuture;
  }

  public void stopRestoreServer() {
    if (server != null) {
      server.close();
      server = null;
    }
  }
}
