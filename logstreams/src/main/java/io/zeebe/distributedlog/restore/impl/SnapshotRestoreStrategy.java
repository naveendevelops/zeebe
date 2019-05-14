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
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.logstreams.state.SnapshotRequester;
import java.util.concurrent.CompletableFuture;

public class SnapshotRestoreStrategy implements RestoreStrategy {

  private final SnapshotRequester requester;
  private MemberId server;
  private RestoreClient client;
  private final LogStream logStream;
  private final SnapshotController procesorSnapshotController;
  private final LogReplicator logReplicator;
  private long backupPosition;

  public SnapshotRestoreStrategy(
      RestoreClient client,
      String localMemberId,
      int partitionId,
      LogStream logStream,
      LogReplicator logReplicator) {
    this.client = client;
    this.logStream = logStream;
    this.logReplicator = logReplicator;
    procesorSnapshotController =
        LogstreamConfig.getProcesorSnapshotController(localMemberId, partitionId);
    final SnapshotController exporterSnapshotController =
        LogstreamConfig.getExporterSnapshotController(localMemberId, partitionId);
    this.requester =
        new SnapshotRequester(client, procesorSnapshotController, exporterSnapshotController);
  }

  @Override
  public CompletableFuture<Long> executeRestoreStrategy() {
    final CompletableFuture<Long> replicated = CompletableFuture.completedFuture(null);
    return replicated
        .thenCompose(nothing -> client.requestSnapshotInfo(server))
        .thenCompose(numSnapshots -> requester.getLatestSnapshotsFrom(server, numSnapshots > 1))
        .thenCompose(nothing -> onSnapshotsReplicated());
  }

  private CompletableFuture<Long> onSnapshotsReplicated() {
    final long lastEventPosition = getValidSnapshotPosition();
    logStream.delete(lastEventPosition);
    logStream.setCommitPosition(lastEventPosition);
    if (lastEventPosition < backupPosition) {
      return logReplicator.replicate(server, lastEventPosition, backupPosition);
    } else {
      return CompletableFuture.completedFuture(lastEventPosition);
    }
  }

  private long getValidSnapshotPosition() {
    if (logStream.getExporterPositionSupplier() != null
        && logStream.getExporterPositionSupplier().get() > 0) {
      return Math.min(
          procesorSnapshotController.getLastValidSnapshotPosition(),
          logStream.getExporterPositionSupplier().get());
    } else {
      return procesorSnapshotController.getLastValidSnapshotPosition();
    }
  }

  public void setBackupPosition(long backupPosition) {
    this.backupPosition = backupPosition;
  }

  public void setServer(MemberId server) {
    this.server = server;
  }
}
