package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.SnapshotController;
import io.zeebe.logstreams.state.SnapshotRequester;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class SnapshotRestoreStrategy implements RestoreStrategy {

  private final SnapshotRequester requester;
  private MemberId server;
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
    this.logStream = logStream;
    this.logReplicator = logReplicator;
    procesorSnapshotController =
        LogstreamConfig.getProcesorSnapshotController(localMemberId, partitionId);
    final SnapshotController exporterSnapshotController =
        LogstreamConfig.getExporterSnapshotController(localMemberId, partitionId);
    this.requester =
        new SnapshotRequester(
            client,
            (SnapshotController[])
                Arrays.asList(procesorSnapshotController, exporterSnapshotController).toArray());
  }

  @Override
  public CompletableFuture<Long> executeRestoreStrategy() {
    final CompletableFuture<Long> replicated = new CompletableFuture<Long>();
    requester
        .getLatestSnapshotsFrom(server)
        .whenComplete(
            (nothing, error) -> {
              if (error == null) {
                replicated.complete(getPosition());
              } else {
                replicated.completeExceptionally(error);
              }
            });
    return replicated.thenCompose(this::replicateEvents);
  }

  private CompletableFuture<Long> replicateEvents(long eventPosition) {
    logStream.delete(eventPosition);
    logStream.setCommitPosition(eventPosition);
    if (eventPosition < backupPosition) {
      return logReplicator.replicate(server, eventPosition, backupPosition);
    } else {
      return CompletableFuture.completedFuture(eventPosition);
    }
  }

  private long getPosition() {
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
