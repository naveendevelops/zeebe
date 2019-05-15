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
package io.zeebe.broker.clustering.base.partitions;

import io.atomix.cluster.messaging.ClusterEventService;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.engine.EngineService;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.engine.state.DefaultZeebeDbFactory;
import io.zeebe.engine.state.StateStorageFactory;
import io.zeebe.engine.state.replication.StateReplication;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.NoneSnapshotReplication;
import io.zeebe.logstreams.state.SnapshotReplication;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;

public class StateSnapshotControllerService implements Service<StateSnapshotController> {
  private final Injector<LogStream> logStreamInjector = new Injector<>();
  private final Injector<StateStorageFactory> stateStorageFactoryInjector = new Injector<>();

  private final ClusterEventService eventService;
  private final RaftState role;
  private final BrokerCfg brokerCfg;
  private final int partitionId;

  private SnapshotReplication stateReplication;
  private StateSnapshotController snapshotController;

  public StateSnapshotControllerService(
      BrokerCfg brokerCfg,
      ClusterEventService eventService,
      final int partitionId,
      final RaftState role) {
    this.brokerCfg = brokerCfg;
    this.partitionId = partitionId;
    this.eventService = eventService;
    this.role = role;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    final String streamProcessorName = EngineService.PROCESSOR_NAME;

    final StateStorageFactory stateStorageFactory = stateStorageFactoryInjector.getValue();
    final StateStorage stateStorage = stateStorageFactory.create(partitionId, streamProcessorName);

    stateReplication =
        shouldReplicateSnapshots()
            ? new StateReplication(eventService, partitionId, streamProcessorName)
            : new NoneSnapshotReplication();

    snapshotController =
        new StateSnapshotController(
            DefaultZeebeDbFactory.DEFAULT_DB_FACTORY,
            stateStorage,
            stateReplication,
            brokerCfg.getData().getMaxSnapshots());

    if (role == RaftState.LEADER) {
      try {
        snapshotController.recover();
      } catch (Exception e) {
        Loggers.SERVICES_LOGGER.error(
            String.format(
                "Unexpected error occurred while recovering snapshot controller on partition %d",
                partitionId),
            e);
      }
    }
  }

  private boolean shouldReplicateSnapshots() {
    return brokerCfg.getCluster().getReplicationFactor() > 1;
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    stateReplication.close();
    if (snapshotController != null) {
      try {
        snapshotController.close();
        snapshotController = null;
      } catch (Exception e) {
        Loggers.SERVICES_LOGGER.error(
            "Unexpected error occurred while closing snapshotController: ", e);
      }
    }
  }

  @Override
  public StateSnapshotController get() {
    return snapshotController;
  }

  public Injector<StateStorageFactory> getStateStorageFactoryInjector() {
    return stateStorageFactoryInjector;
  }
}
