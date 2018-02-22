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
package io.zeebe.util.sched.metrics;

import static io.zeebe.util.sched.metrics.SchedulerMetrics.TYPE_TEMPORAL_VALUE;

import java.util.Arrays;

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * Actor runner metrics
 *
 */
public class ActorRunnerMetrics implements AutoCloseable
{
    private final AtomicCounter runnerIdleTime;
    private final AtomicCounter runnerBusyTime;
    private final AtomicCounter jobExecutionCount;
    private final AtomicCounter taskStealCount;
    private final AtomicCounter[] taskPriorityCounters;

    public ActorRunnerMetrics(String runnerName, CountersManager countersManager, int priorityCount)
    {
        runnerIdleTime = countersManager.newCounter(String.format("%s.runnerIdleTime", runnerName), TYPE_TEMPORAL_VALUE);
        runnerBusyTime = countersManager.newCounter(String.format("%s.runnerBusyTime", runnerName), TYPE_TEMPORAL_VALUE);
        jobExecutionCount = countersManager.newCounter(String.format("%s.jobCount", runnerName));
        taskStealCount = countersManager.newCounter(String.format("%s.taskStealCount", runnerName));

        taskPriorityCounters = new AtomicCounter[priorityCount];
        for (int i = 0; i < taskPriorityCounters.length; i++)
        {
            taskPriorityCounters[i] = countersManager.newCounter(String.format("%s.taskExecutionCount-%d", runnerName, i));
        }
    }

    public void incrementTaskStealCount()
    {
        taskStealCount.incrementOrdered();
    }

    public void incrementTaskExecutionCount(int priority)
    {
        taskPriorityCounters[priority].incrementOrdered();
    }

    public void incrementJobCount()
    {
        jobExecutionCount.incrementOrdered();
    }

    public void recordRunnerIdleTime(long time)
    {
        runnerIdleTime.getAndAddOrdered(time);
    }

    public void recordRunnerBusyTime(long time)
    {
        runnerBusyTime.getAndAddOrdered(time);
    }

    @Override
    public void close()
    {
        jobExecutionCount.close();
        taskStealCount.close();
        runnerIdleTime.close();
        runnerBusyTime.close();
        Arrays.asList(taskPriorityCounters).forEach(AtomicCounter::close);
    }
}
