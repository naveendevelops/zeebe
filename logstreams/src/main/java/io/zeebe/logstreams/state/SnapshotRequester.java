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
import io.zeebe.logstreams.spi.SnapshotController;
import java.util.concurrent.CompletableFuture;

// TODO: handle case where not all snapshot controllers are needed, e.g. no exporter snapshot on
// leader
public class SnapshotRequester {
  private final RestoreClient client;
  private final SnapshotController[] snapshotControllers;

  public SnapshotRequester(RestoreClient client, SnapshotController[] snapshotControllers) {
    this.client = client;
    this.snapshotControllers = snapshotControllers;
  }

  public CompletableFuture<Void> getLatestSnapshotsFrom(MemberId server) {
    CompletableFuture<Void> replicated = CompletableFuture.completedFuture(null);

    for (SnapshotController controller : snapshotControllers) {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      controller.addListener(new DefaultSnapshotReplicationListener(controller, future));
      replicated = replicated.thenCompose((nothing) -> future);
    }

    client.requestLatestSnapshot(server);
    return replicated;
  }

  static class DefaultSnapshotReplicationListener implements SnapshotReplicationListener {
    private final SnapshotController controller;
    private final CompletableFuture<Void> future;

    DefaultSnapshotReplicationListener(
        SnapshotController controller, CompletableFuture<Void> future) {
      this.controller = controller;
      this.future = future;
    }

    @Override
    public void onReplicated(long snapshotPosition) {
      future.complete(null);
      controller.removeListener(this);
    }

    @Override
    public void onFailure(long snapshotPosition) {
      future.completeExceptionally(new FailedSnapshotReplication(snapshotPosition));
      controller.enableRetrySnapshot(snapshotPosition);
      controller.removeListener(this);
    }
  }
}
