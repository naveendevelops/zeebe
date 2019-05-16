/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreClientFactory;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.ReplicationController;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.SnapshotRequester;
import io.zeebe.logstreams.state.StateStorage;
import java.io.File;
import java.util.concurrent.CompletableFuture;

public class SnapshotRestoreStrategy implements RestoreStrategy {

  private SnapshotRequester requester;
  private MemberId server;
  private final RestoreClientFactory restoreClientFactory;
  private RestoreClient client;
  private int partitionId;
  private final LogStream logStream;
  private final LogReplicator logReplicator;
  private StorageConfiguration configuration;
  private long backupPosition;
  private ReplicationController processorSnapshotReplication;
  private ReplicationController exporterSnapshotReplication;
  private ClusterCommunicationService communicationService;
  private SnapshotReplication processorSnapshotReplicationConsumer;
  private SnapshotReplication exporterSnapshotReplicationConsumer;

  public SnapshotRestoreStrategy(
      RestoreClientFactory restoreClientFactory,
      RestoreClient client,
      int partitionId,
      LogStream logStream,
      LogReplicator logReplicator,
      StorageConfiguration configuration) {
    this.restoreClientFactory = restoreClientFactory;
    this.client = client;
    this.partitionId = partitionId;
    this.logStream = logStream;
    this.logReplicator = logReplicator;
    this.configuration = configuration;
    getSnapshotControllers(client);
  }

  private void getSnapshotControllers(RestoreClient client) {
    exporterSnapshotReplicationConsumer =
        restoreClientFactory.createExporterSnapshotReplicationConsumer(partitionId);
    processorSnapshotReplicationConsumer =
        restoreClientFactory.createProcessorSnapshotReplicationConsumer(partitionId);
    StateStorage storage =
        createStorage(
            configuration.getStatesDirectory().toString(), partitionId, "zb-stream-processor");
    processorSnapshotReplication = // TODO: pass Correct StateStorage directory
        new ReplicationController(
            exporterSnapshotReplicationConsumer, storage, () -> {}, () -> -1L);
    exporterSnapshotReplication =
        new ReplicationController(
            processorSnapshotReplicationConsumer, storage, () -> {}, () -> -1L);

    this.requester =
        new SnapshotRequester(client, processorSnapshotReplication, exporterSnapshotReplication);
  }

  // FIXME: this method is copied from StateStorageFactory.
  private StateStorage createStorage(
      String rootDirectory, final int processorId, final String processorName) {
    final String name = String.format("%d_%s", processorId, processorName);
    final File processorDirectory = new File(rootDirectory, name);

    final File runtimeDirectory = new File(processorDirectory, "runtime");
    final File snapshotsDirectory = new File(processorDirectory, "snapshots");

    if (!processorDirectory.exists()) {
      processorDirectory.mkdir();
    }

    if (!snapshotsDirectory.exists()) {
      snapshotsDirectory.mkdir();
    }

    return new StateStorage(runtimeDirectory, snapshotsDirectory);
  }

  @Override
  public CompletableFuture<Long> executeRestoreStrategy() {
    final CompletableFuture<Long> replicated = CompletableFuture.completedFuture(null);
    processorSnapshotReplication.consumeReplicatedSnapshots(pos -> {});
    exporterSnapshotReplication.consumeReplicatedSnapshots(pos -> {});
    return replicated
        .thenCompose(nothing -> client.requestSnapshotInfo(server))
        .thenCompose(numSnapshots -> requester.getLatestSnapshotsFrom(server, numSnapshots > 1))
        .thenCompose(processorSnapshotPosition -> onSnapshotsReplicated(processorSnapshotPosition));
  }

  private CompletableFuture<Long> onSnapshotsReplicated(long processorSnapshotPosition) {
    processorSnapshotReplicationConsumer.close();
    exporterSnapshotReplicationConsumer.close();
    final long lastEventPosition = getValidSnapshotPosition(processorSnapshotPosition);
    logStream.delete(lastEventPosition);
    logStream.setCommitPosition(lastEventPosition);
    if (lastEventPosition < backupPosition) {
      return logReplicator.replicate(server, lastEventPosition, backupPosition);
    } else {
      return CompletableFuture.completedFuture(lastEventPosition);
    }
  }

  private long getValidSnapshotPosition(long processorSnapshotPosition) {
    if (logStream.getExporterPositionSupplier() != null
        && logStream.getExporterPositionSupplier().get() > 0) {
      return Math.min(processorSnapshotPosition, logStream.getExporterPositionSupplier().get());
    } else {
      return processorSnapshotPosition;
    }
  }

  public void setBackupPosition(long backupPosition) {
    this.backupPosition = backupPosition;
  }

  public void setServer(MemberId server) {
    this.server = server;
  }
}
