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
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreClientFactory;
import io.zeebe.distributedlog.restore.RestoreServer;

public class BrokerRestoreFactory implements RestoreClientFactory {
  private final ClusterCommunicationService communicationService;
  private final String replicationTopic;
  private final String restoreInfoTopic;
  private final String snapshotRequestTopic;

  public BrokerRestoreFactory(ClusterCommunicationService communicationService, int partitionId) {
    this.communicationService = communicationService;
    this.replicationTopic = String.format("log-replication-%d", partitionId);
    this.restoreInfoTopic = String.format("restore-info-%d", partitionId);
    this.snapshotRequestTopic = String.format("snapshot-request-%d", partitionId);
  }

  @Override
  public RestoreClient createClient() {
    return new BrokerRestoreClient(
        communicationService, replicationTopic, restoreInfoTopic, snapshotRequestTopic);
  }

  public RestoreServer createServer() {
    return new BrokerRestoreServer(
        communicationService, replicationTopic, restoreInfoTopic, snapshotRequestTopic);
  }
}
