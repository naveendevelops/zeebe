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
package io.zeebe.broker.it.fault;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.BpmnElementType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.util.ByteValue;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class RestoreTest {
  private static final int ATOMIX_SEGMENT_SIZE = (int) ByteValue.ofMegabytes(32).toBytes();
  private static final int LARGE_PAYLOAD_BYTESIZE = (int) ByteValue.ofKilobytes(32).toBytes();
  private static final String LARGE_PAYLOAD =
      "{\"blob\": \"" + getRandomBase64Bytes(LARGE_PAYLOAD_BYTESIZE) + "\"}";
  private final ClusteringRule clusteringRule =
      new ClusteringRule(
          1,
          3,
          3,
          cfg -> {
            cfg.getData().setMaxSnapshots(1);
            cfg.getData().setSnapshotPeriod("15m");
            cfg.getData().setSnapshotReplicationPeriod("15m");
            cfg.getData().setDefaultLogSegmentSize("32M"); // same as Atomix segment size
          });
  private final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(clusteringRule).around(clientRule);

  @Test
  public void shouldReplicateLogEvents() {
    // given
    final Broker node1 = clusteringRule.getBroker(1);
    final BpmnModelInstance workflow =
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("task", b -> b.zeebeTaskType("type"))
            .endEvent()
            .done();

    // when
    final long workflowKey = clientRule.deployWorkflow(workflow);
    final int requiredInstances = Math.floorDiv(ATOMIX_SEGMENT_SIZE, LARGE_PAYLOAD_BYTESIZE) + 1;
    IntStream.range(0, requiredInstances)
        .forEach(i -> clientRule.createWorkflowInstance(workflowKey, LARGE_PAYLOAD));

    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withWorkflowKey(workflowKey)
                .withElementType(BpmnElementType.PROCESS)
                .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
                .count())
        .isEqualTo(requiredInstances);
  }

  private static String getRandomBase64Bytes(long size) {
    final byte[] bytes = new byte[(int) size];
    ThreadLocalRandom.current().nextBytes(bytes);

    return Base64.getEncoder().encodeToString(bytes);
  }
}
