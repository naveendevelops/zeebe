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

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.partition.Partition;
import io.zeebe.broker.engine.EngineService;
import io.zeebe.broker.exporter.ExporterManagerService;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreInfoRequest;
import io.zeebe.distributedlog.restore.RestoreInfoResponse;
import io.zeebe.distributedlog.restore.log.LogReplicationRequest;
import io.zeebe.distributedlog.restore.log.LogReplicationResponse;
import io.zeebe.engine.state.StateStorageFactory;
import io.zeebe.engine.state.replication.StateReplication;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.StateStorage;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class BrokerRestoreClient implements RestoreClient {
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(5);
  private final ClusterCommunicationService communicationService;
  private final Partition partition;
  private final String logReplicationTopic;
  private final String restoreInfoTopic;
  private final String snapshotRequestTopic;
  private final String snapshotInfoRequestTopic;
  private final ClusterEventService eventService;
  private String localMemberId;

  public BrokerRestoreClient(
      ClusterCommunicationService communicationService,
      String localMemberId,
      Partition partition,
      String logReplicationTopic,
      String restoreInfoTopic,
      String snapshotRequestTopic,
      String snapshotInfoRequestTopic,
      ClusterEventService eventService) {
    this.communicationService = communicationService;
    this.localMemberId = localMemberId;
    this.partition = partition;
    this.logReplicationTopic = logReplicationTopic;
    this.restoreInfoTopic = restoreInfoTopic;
    this.snapshotRequestTopic = snapshotRequestTopic;
    this.snapshotInfoRequestTopic = snapshotInfoRequestTopic;
    this.eventService = eventService;
  }

  @Override
  public Collection<MemberId> getPartitionMembers() {
    return partition.members();
  }

  @Override
  public SnapshotReplication createProcessorSnapshotReplicationConsumer(int partitionId) {
    return new StateReplication(eventService, partitionId, EngineService.PROCESSOR_NAME);
  }

  @Override
  public SnapshotReplication createExporterSnapshotReplicationConsumer(int partitionId) {
    return new StateReplication(eventService, partitionId, ExporterManagerService.PROCESSOR_NAME);
  }

  @Override
  public StateStorage getProcessorStateStorage(int partitionId) {
    final StorageConfiguration configuration =
        LogstreamConfig.getConfig(localMemberId, partitionId).join();
    return new StateStorageFactory(configuration.getStatesDirectory())
        .create(partitionId, EngineService.PROCESSOR_NAME);
  }

  @Override
  public StateStorage getExporterStateStorage(int partitionId) {
    final StorageConfiguration configuration =
        LogstreamConfig.getConfig(localMemberId, partitionId).join();
    return new StateStorageFactory(configuration.getStatesDirectory())
        .create(
            ExporterManagerService.EXPORTER_PROCESSOR_ID, ExporterManagerService.PROCESSOR_NAME);
  }

  @Override
  public CompletableFuture<LogReplicationResponse> requestLogReplication(
      MemberId server, LogReplicationRequest request) {
    return communicationService.send(
        logReplicationTopic,
        request,
        SbeLogReplicationRequest::serialize,
        SbeLogReplicationResponse::new,
        server,
        DEFAULT_REQUEST_TIMEOUT);
  }

  @Override
  public CompletableFuture<RestoreInfoResponse> requestRestoreInfo(
      MemberId server, RestoreInfoRequest request) {
    return communicationService.send(
        restoreInfoTopic,
        request,
        SbeRestoreInfoRequest::serialize,
        SbeRestoreInfoResponse::new,
        server,
        DEFAULT_REQUEST_TIMEOUT);
  }

  @Override
  public CompletableFuture<Integer> requestSnapshotInfo(MemberId server) {
    return communicationService.send(
        snapshotInfoRequestTopic, null, server, DEFAULT_REQUEST_TIMEOUT);
  }

  @Override
  public void requestLatestSnapshot(MemberId server) {
    communicationService.unicast(snapshotRequestTopic, null, server);
  }
}
