/*
 * Zeebe Workflow Engine
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
package io.zeebe.engine.processor.workflow.deployment;

import static io.zeebe.protocol.intent.DeploymentIntent.CREATE;

import io.zeebe.engine.processor.TypedRecordProcessors;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.deployment.WorkflowState;
import io.zeebe.protocol.clientapi.ValueType;

public class DeploymentEventProcessors {

  public static void addDeploymentCreateProcessor(
      TypedRecordProcessors typedRecordProcessors, WorkflowState workflowState) {
    typedRecordProcessors.onCommand(
        ValueType.DEPLOYMENT, CREATE, new DeploymentCreateProcessor(workflowState));
  }

  public static void addTransformingDeploymentProcessor(
      TypedRecordProcessors typedRecordProcessors,
      ZeebeState zeebeState,
      CatchEventBehavior catchEventBehavior) {
    typedRecordProcessors.onCommand(
        ValueType.DEPLOYMENT,
        CREATE,
        new TransformingDeploymentCreateProcessor(zeebeState, catchEventBehavior));
  }
}
