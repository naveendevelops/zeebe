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

import static org.mockito.Mockito.spy;

import io.zeebe.engine.state.ZeebeState;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.util.sched.future.ActorFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;

public class RecordingStreamProcessor implements TypedRecordProcessor {

  private final List<UnpackedObject> events = new ArrayList<>();
  private final AtomicInteger processedEvents = new AtomicInteger(0);

  private final ActorFuture<Void> openFuture;

  private ProcessingContext context = null;

  public RecordingStreamProcessor(ZeebeState zeebeState, ActorFuture<Void> openFuture) {
    this.openFuture = openFuture;
  }

  @Override
  public void onOpen(ProcessingContext context) {
    this.context = context;
    openFuture.complete(null);
  }

  @Override
  public void processRecord(
      TypedRecord record, TypedResponseWriter responseWriter, TypedStreamWriter streamWriter) {

    final UnpackedObject value = record.getValue();
    final int valueLength = value.getLength();
    final UnsafeBuffer cloneBuffer = new UnsafeBuffer(new byte[valueLength]);
    value.write(cloneBuffer, 0);

    final UnpackedObject clone = new UnpackedObject();
    clone.wrap(cloneBuffer);
    events.add(clone);

    processedEvents.incrementAndGet();
  }

  public static RecordingStreamProcessor createSpy(
      ZeebeState zeebeState, ActorFuture<Void> openFuture) {
    return spy(new RecordingStreamProcessor(zeebeState, openFuture));
  }

  public List<UnpackedObject> getEvents() {
    return events;
  }

  public int getProcessedEventCount() {
    return processedEvents.get();
  }

  public ProcessingContext getContext() {
    return context;
  }
}
