package io.zeebe.broker.it.fault;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.test.util.MsgPackUtil;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.DirectBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class RestoreTest {
  private final ClusteringRule clusteringRule =
      new ClusteringRule(
          3,
          3,
          3,
          cfg -> {
            cfg.getData().setMaxSnapshots(1);
            cfg.getData().setSnapshotPeriod("15M");
            cfg.getData().setSnapshotReplicationPeriod("15M");
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
    final String blob = getRandomBase64Bytes(50 * 1024 * 1024);
    final DirectBuffer variables = MsgPackUtil.asMsgPack("{'blob': '" + blob + '"');

    // when
    final long workflowKey = clientRule.deployWorkflow(workflow);
    clientRule.createWorkflowInstance(workflowKey, variables);
    clusteringRule.stopBroker(1);
  }

  private String getRandomBase64Bytes(int size) {
    final byte[] bytes = new byte[size];
    ThreadLocalRandom.current().nextBytes(bytes);

    return Base64.getEncoder().encodeToString(bytes);
  }
}
