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

import io.atomix.core.Atomix;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.partitions.PartitionLeaderElection;
import io.zeebe.broker.clustering.base.partitions.RaftState;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreInfoServer;
import io.zeebe.distributedlog.restore.RestoreServer;
import io.zeebe.distributedlog.restore.impl.DefaultRestoreInfoServerHandler;
import io.zeebe.distributedlog.restore.log.LogReplicationServer;
import io.zeebe.distributedlog.restore.log.impl.DefaultLogReplicationServerHandler;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.concurrent.ExecutorService;

public class PartitionRestoreService implements Service<Void> {
  private final Injector<Atomix> atomixInjector = new Injector<>();
  private final Injector<PartitionLeaderElection> leaderElectionInjector = new Injector<>();
  private final Injector<Partition> partitionInjector = new Injector<>();

  private final ExecutorService executor;

  private PartitionLeaderElection election;
  private BrokerRestoreFactory restoreFactory;
  private Partition partition;
  private RestoreServer server;
  private String localMemberId;

  public PartitionRestoreService(ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public Void get() {
    return null;
  }

  @Override
  public void start(ServiceStartContext startContext) {
    final Atomix atomix = atomixInjector.getValue();
    localMemberId = atomix.getMembershipService().getLocalMember().id().id();
    final int partitionId = partition.getPartitionId();

    partition = partitionInjector.getValue();
    election = leaderElectionInjector.getValue();
    restoreFactory = new BrokerRestoreFactory(atomix.getCommunicationService(), partitionId);

    if (partition.getState() == RaftState.LEADER) {
      startContext.async(startRestoreServer());
    }

    LogstreamConfig.putLeaderElectionController(localMemberId, partitionId, election);
    LogstreamConfig.putRestoreClientFactory(localMemberId, partitionId, restoreFactory);
  }

  @Override
  public void stop(ServiceStopContext stopContext) {
    final int partitionId = partition.getPartitionId();

    if (server != null) {
      server.close();
    }

    LogstreamConfig.removeLeaderElectionController(localMemberId, partitionId);
    LogstreamConfig.removeRestoreClientFactory(localMemberId, partitionId);
  }

  private CompletableActorFuture<Void> startRestoreServer() {
    final CompletableActorFuture<Void> startedFuture = new CompletableActorFuture<>();
    final LogReplicationServer.Handler logReplicationHandler =
        new DefaultLogReplicationServerHandler(partition.getLogStream());
    final RestoreInfoServer.Handler restoreInfoHandler =
        new DefaultRestoreInfoServerHandler(
            partition.getLogStream(), partition.getProcessorSnapshotController());

    server = restoreFactory.createServer(executor);
    server
        .serve(logReplicationHandler)
        .thenCompose(nothing -> server.serve(restoreInfoHandler))
        .thenAccept(startedFuture::complete);
    return startedFuture;
  }

  public Injector<Atomix> getAtomixInjector() {
    return atomixInjector;
  }

  public Injector<PartitionLeaderElection> getLeaderElectionInjector() {
    return leaderElectionInjector;
  }

  public Injector<Partition> getPartitionInjector() {
    return partitionInjector;
  }
}
