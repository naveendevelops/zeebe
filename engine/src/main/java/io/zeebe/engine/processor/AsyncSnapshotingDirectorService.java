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
package io.zeebe.engine.processor;

import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import java.time.Duration;

public class AsyncSnapshotingDirectorService implements Service<AsyncSnapshotingDirectorService> {
  private final Injector<StreamProcessorService> streamProcessorServiceInjector = new Injector<>();

  private final StateSnapshotController snapshotController;
  private final LogStream logStream;
  private final Duration snapshotPeriod;
  private final int maxSnapshots;
  private AsyncSnapshotDirector asyncSnapshotDirector;

  public AsyncSnapshotingDirectorService(
      final StateSnapshotController snapshotController,
      final LogStream logStream,
      Duration snapshotPeriod,
      int maxSnapshots) {
    this.snapshotController = snapshotController;
    this.logStream = logStream;
    this.snapshotPeriod = snapshotPeriod;
    this.maxSnapshots = maxSnapshots;
  }

  @Override
  public void start(final ServiceStartContext startContext) {
    asyncSnapshotDirector =
        new AsyncSnapshotDirector(
            streamProcessorServiceInjector.getValue().getController(),
            snapshotController,
            logStream,
            snapshotPeriod,
            maxSnapshots);

    startContext.getScheduler().submitActor(asyncSnapshotDirector);
  }

  @Override
  public void stop(final ServiceStopContext stopContext) {
    stopContext.run(
        () -> {
          if (asyncSnapshotDirector != null) {
            asyncSnapshotDirector.close();
            asyncSnapshotDirector = null;
          }
        });
  }

  @Override
  public AsyncSnapshotingDirectorService get() {
    return this;
  }

  public Injector<StreamProcessorService> getStreamProcessorServiceInjector() {
    return streamProcessorServiceInjector;
  }
}
