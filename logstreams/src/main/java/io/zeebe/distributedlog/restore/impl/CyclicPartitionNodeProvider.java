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
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.RaftClusterContext;
import io.zeebe.distributedlog.restore.RestoreNodeProvider;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Iterates over partition members cyclically, skipping the local node. After each loop, it will
 * refresh the list of members it should use.
 */
public class CyclicPartitionNodeProvider implements RestoreNodeProvider {
  private final RaftClusterContext clusterContext;
  private final MemberId localMemberId;
  private final Queue<MemberId> members;

  public CyclicPartitionNodeProvider(RaftClusterContext clusterContext, MemberId localMemberId) {
    this.clusterContext = clusterContext;
    this.localMemberId = localMemberId;
    this.members = new ArrayDeque<>();
  }

  @Override
  public MemberId provideRestoreNode() {
    return memberQueue().poll();
  }

  private Queue<MemberId> memberQueue() {
    if (members.isEmpty()) {
      clusterContext.getMembers().stream()
          .map(RaftMember::memberId)
          .filter(m -> !m.equals(localMemberId))
          .forEach(members::add);
    }

    return members;
  }
}
