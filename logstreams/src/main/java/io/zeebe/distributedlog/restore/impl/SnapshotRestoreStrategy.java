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
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreContext;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.ReplicationController;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.SnapshotRequester;
import io.zeebe.logstreams.state.StateStorage;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;

public class SnapshotRestoreStrategy implements RestoreStrategy {

  private SnapshotRequester requester;
  private MemberId server;
  private final SnapshotRestoreContext restoreContext;
  private RestoreClient client;
  private int partitionId;
  private final LogStream logStream;
  private final LogReplicator logReplicator;
  private StorageConfiguration configuration;
  private long backupPosition;
  private ReplicationController processorSnapshotReplication;
  private ReplicationController exporterSnapshotReplication;
  private SnapshotReplication processorSnapshotReplicationConsumer;
  private SnapshotReplication exporterSnapshotReplicationConsumer;
  private StateStorage exporterStorage;
  private long latestLocalPosition;

  public SnapshotRestoreStrategy(
      RestoreClient client,
      int partitionId,
      LogStream logStream,
      LogReplicator logReplicator,
      StorageConfiguration configuration) {
    this.client = client;
    this.restoreContext = client.createSnapshotRestoreContext();
    this.partitionId = partitionId;
    this.logStream = logStream;
    this.logReplicator = logReplicator;
    this.configuration = configuration;
    getSnapshotControllers(client);
  }

  private void getSnapshotControllers(RestoreClient client) {
    exporterSnapshotReplicationConsumer =
        restoreContext.createExporterSnapshotReplicationConsumer(partitionId);
    processorSnapshotReplicationConsumer =
        restoreContext.createProcessorSnapshotReplicationConsumer(partitionId);
    final StateStorage processorTmpStorage =
        createTmpStateDirectory(configuration.getStatesDirectory().toString(), "processor");
    final StateStorage exporterTmpStorage =
        createTmpStateDirectory(configuration.getStatesDirectory().toString(), "exporter");

    processorSnapshotReplication =
        new ReplicationController(
            processorSnapshotReplicationConsumer, processorTmpStorage, () -> {}, () -> -1L);

    exporterStorage = restoreContext.getExporterStateStorage(partitionId);
    exporterSnapshotReplication =
        new ReplicationController(
            exporterSnapshotReplicationConsumer, exporterTmpStorage, () -> {}, () -> -1L);

    final StateStorage processorStorage = restoreContext.getProcessorStateStorage(partitionId);
    this.requester =
        new SnapshotRequester(
            client,
            latestLocalPosition,
            processorSnapshotReplication,
            exporterSnapshotReplication,
            pos -> moveValidSnapshot(pos, processorTmpStorage, processorStorage),
            pos -> moveValidSnapshot(pos, exporterTmpStorage, exporterStorage));
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
    // logStream.delete(lastEventPosition);
    LoggerFactory.getLogger("Snapshot restore")
        .info("Snapshot replicated {}, backup position {}", lastEventPosition, backupPosition);
    logStream.setCommitPosition(lastEventPosition);
    if (lastEventPosition < backupPosition) {
      return logReplicator.replicate(server, lastEventPosition, backupPosition);
    } else {
      return CompletableFuture.completedFuture(lastEventPosition);
    }
  }

  private long getValidSnapshotPosition(long processorSnapshotPosition) {
    final Supplier<Long> exporterPositionSupplier =
        restoreContext.getExporterPositionSupplier(exporterStorage);
    if (exporterPositionSupplier != null) {
      final long exporterPosition = exporterPositionSupplier.get();
      return Math.min(processorSnapshotPosition, exporterPosition);
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

  private StateStorage createTmpStateDirectory(final String rootDirectory, final String name) {
    final File processorDirectory = new File(rootDirectory, "restore-" + name);
    if (!processorDirectory.exists()) {
      processorDirectory.mkdirs();
    }
    return new StateStorage(processorDirectory.toString());
  }

  private void moveValidSnapshot(
      long snapshotPosition, StateStorage tmpStorage, StateStorage storage) {
    try {
      Files.move(
          tmpStorage.getSnapshotDirectoryFor(snapshotPosition).toPath(),
          storage.getSnapshotDirectoryFor(snapshotPosition).toPath());
    } catch (FileAlreadyExistsException e) {

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setLatestLocalPosition(long latestLocalPosition) {
    this.latestLocalPosition = latestLocalPosition;
  }
}
