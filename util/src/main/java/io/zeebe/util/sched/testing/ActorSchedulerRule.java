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
package io.zeebe.util.sched.testing;

import io.zeebe.util.sched.ZbActor;
import io.zeebe.util.sched.ZbActorScheduler;
import io.zeebe.util.sched.ZbActorScheduler.ActorSchedulerBuilder;
import io.zeebe.util.sched.clock.ActorClock;
import org.junit.rules.ExternalResource;

public class ActorSchedulerRule extends ExternalResource
{
    private final ZbActorScheduler actorScheduler;
    private ActorSchedulerBuilder builder;

    public ActorSchedulerRule(int numOfThreads, ActorClock clock)
    {
        builder = ZbActorScheduler.newActorScheduler()
            .setActorThreadCount(numOfThreads)
            .setActorClock(clock);

        actorScheduler = builder
            .build();
    }

    public ActorSchedulerRule(int numOfThreads)
    {
        this(numOfThreads, null);
    }

    public ActorSchedulerRule(ActorClock clock)
    {
        this(Math.min(1, Runtime.getRuntime().availableProcessors() - 1), clock);
    }


    public ActorSchedulerRule()
    {
        this(null);
    }

    @Override
    protected void before() throws Throwable
    {
        actorScheduler.start();
    }

    @Override
    protected void after()
    {
        actorScheduler.stop();
    }

    public void submitActor(ZbActor actor)
    {
        actorScheduler.submitActor(actor);
    }

    public void submitActor(ZbActor actor, boolean useCountersManager)
    {
        actorScheduler.submitActor(actor, useCountersManager);
    }

    public ZbActorScheduler get()
    {
        return actorScheduler;
    }

    public ActorSchedulerBuilder getBuilder()
    {
        return builder;
    }
}
