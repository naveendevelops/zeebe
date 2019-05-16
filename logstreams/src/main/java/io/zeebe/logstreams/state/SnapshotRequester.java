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
package io.zeebe.logstreams.state;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import java.util.concurrent.CompletableFuture;
import org.slf4j.LoggerFactory;

// TODO: handle case where not all snapshot controllers are needed, e.g. no exporter snapshot on
// leader
public class SnapshotRequester {
  private final RestoreClient client;
  private final ReplicationController processorSnapshotController;
  private final ReplicationController exporterSnapshotController;

  public SnapshotRequester(
      RestoreClient client,
      ReplicationController processorSnapshotController,
      ReplicationController exporterSnapshotController) {
    this.client = client;
    this.processorSnapshotController = processorSnapshotController;
    this.exporterSnapshotController = exporterSnapshotController;
  }

  public CompletableFuture<Long> getLatestSnapshotsFrom(
      MemberId server, boolean getExporterSnapshot) {
    CompletableFuture<Long> replicated = CompletableFuture.completedFuture(null);

    if (getExporterSnapshot) {
      final CompletableFuture<Long> exporterFuture = new CompletableFuture<>();
      exporterSnapshotController.addListener(
          new DefaultSnapshotReplicationListener(exporterSnapshotController, exporterFuture));
      replicated = replicated.thenCompose((nothing) -> exporterFuture);
    }

    final CompletableFuture<Long> future = new CompletableFuture<>();
    processorSnapshotController.addListener(
        new DefaultSnapshotReplicationListener(processorSnapshotController, future));
    replicated = replicated.thenCompose((nothing) -> future);

    client.requestLatestSnapshot(server);
    return replicated;
  }

  static class DefaultSnapshotReplicationListener implements SnapshotReplicationListener {
    private final ReplicationController controller;
    private final CompletableFuture<Long> future;

    DefaultSnapshotReplicationListener(
        ReplicationController controller, CompletableFuture<Long> future) {
      this.controller = controller;
      this.future = future;
    }

    @Override
    public void onReplicated(long snapshotPosition) {
      LoggerFactory.getLogger("Restore").info("Replicated snapshot {}", snapshotPosition);
      future.complete(snapshotPosition);
      controller.removeListener(this);
    }

    @Override
    public void onFailure(long snapshotPosition) {
      future.completeExceptionally(new FailedSnapshotReplication(snapshotPosition));
      controller.clearInvalidatedSnapshot(snapshotPosition);
      controller.removeListener(this);
    }
  }
}
