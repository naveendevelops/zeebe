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
import java.util.function.Consumer;
import org.slf4j.LoggerFactory;

public class SnapshotRequester {
  private final RestoreClient client;
  private final ReplicationController processorSnapshotController;
  private final ReplicationController exporterSnapshotController;
  private final Consumer<Long> processorSnapshotConsumer;
  private final Consumer<Long> exporterSnapshotConsumer;

  public SnapshotRequester(
      RestoreClient client,
      ReplicationController processorSnapshotController,
      ReplicationController exporterSnapshotController,
      Consumer<Long> processorSnapshotConsumer,
      Consumer<Long> exporterSnapshotConsumer) {
    this.client = client;
    this.processorSnapshotController = processorSnapshotController;
    this.exporterSnapshotController = exporterSnapshotController;
    this.processorSnapshotConsumer = processorSnapshotConsumer;
    this.exporterSnapshotConsumer = exporterSnapshotConsumer;
  }

  public CompletableFuture<Long> getLatestSnapshotsFrom(
      MemberId server, boolean getExporterSnapshot) {
    CompletableFuture<Long> replicated = CompletableFuture.completedFuture(null);

    if (getExporterSnapshot) {
      final CompletableFuture<Long> exporterFuture = new CompletableFuture<>();
      exporterSnapshotController.addListener(
          new DefaultSnapshotReplicationListener(
              exporterSnapshotController, exporterFuture, exporterSnapshotConsumer));
      replicated = replicated.thenCompose((nothing) -> exporterFuture);
    }

    final CompletableFuture<Long> future = new CompletableFuture<>();
    processorSnapshotController.addListener(
        new DefaultSnapshotReplicationListener(
            processorSnapshotController, future, processorSnapshotConsumer));
    replicated = replicated.thenCompose((nothing) -> future);

    client.requestLatestSnapshot(server);
    return replicated;
  }

  static class DefaultSnapshotReplicationListener implements SnapshotReplicationListener {
    private final ReplicationController controller;
    private final CompletableFuture<Long> future;
    private Consumer<Long> snapshotConsumer;

    DefaultSnapshotReplicationListener(
        ReplicationController controller,
        CompletableFuture<Long> future,
        Consumer<Long> snapshotConsumer) {
      this.controller = controller;
      this.future = future;
      this.snapshotConsumer = snapshotConsumer;
    }

    @Override
    public void onReplicated(long snapshotPosition) {
      LoggerFactory.getLogger("Restore").info("Replicated snapshot {}", snapshotPosition);
      try {
        snapshotConsumer.accept(snapshotPosition);
        future.complete(snapshotPosition);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }

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
