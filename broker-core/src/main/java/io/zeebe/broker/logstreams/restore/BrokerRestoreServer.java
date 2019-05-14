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
import io.zeebe.distributedlog.restore.RestoreServer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerRestoreServer implements RestoreServer {
  private final ClusterCommunicationService communicationService;
  private final String logReplicationTopic;
  private final String restoreInfoTopic;
  private final String snapshotRequestTopic;
  private final ExecutorService executor;

  public BrokerRestoreServer(
      ClusterCommunicationService communicationService,
      String logReplicationTopic,
      String restoreInfoTopic,
      String snapshotRequestTopic) {
    this.communicationService = communicationService;
    this.logReplicationTopic = logReplicationTopic;
    this.restoreInfoTopic = restoreInfoTopic;
    this.snapshotRequestTopic = snapshotRequestTopic;

    this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "restore-server"));
  }

  @Override
  public void close() {
    communicationService.unsubscribe(logReplicationTopic);
    communicationService.unsubscribe(restoreInfoTopic);
    executor.shutdownNow();
  }

  @Override
  public CompletableFuture<Void> serve(LogReplicationRequestHandler server) {
    return communicationService.subscribe(
        logReplicationTopic,
        SbeLogReplicationRequest::new,
        server::onReplicationRequest,
        SbeLogReplicationResponse::serialize,
        executor);
  }

  @Override
  public CompletableFuture<Void> serve(RestoreInfoRequestHandler server) {
    return communicationService.subscribe(
        restoreInfoTopic,
        SbeRestoreInfoRequest::new,
        server::onRestoreInfoRequest,
        SbeRestoreInfoResponse::serialize,
        executor);
  }

  @Override
  public CompletableFuture<Void> serve(SnapshotRequestHandler handler) {
    return communicationService.subscribe(
        snapshotRequestTopic, handler::onSnapshotRequest, executor);
  }
}
