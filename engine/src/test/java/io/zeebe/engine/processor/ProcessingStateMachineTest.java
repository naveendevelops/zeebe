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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import io.zeebe.db.DbContext;
import io.zeebe.db.TransactionOperation;
import io.zeebe.db.ZeebeDbTransaction;
import io.zeebe.engine.util.TestStreams;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamReader;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.servicecontainer.testing.ServiceContainerRule;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;

public class ProcessingStateMachineTest {

  public ControlledActorSchedulerRule actorSchedulerRule = new ControlledActorSchedulerRule();

  private ProcessingStateMachine processingStateMachine;
  private TestStreams streams;
  public TemporaryFolder tempFolder = new TemporaryFolder();
  public AutoCloseableRule closeables = new AutoCloseableRule();
  public ServiceContainerRule serviceContainerRule = new ServiceContainerRule(actorSchedulerRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(tempFolder)
          .around(actorSchedulerRule)
          .around(serviceContainerRule)
          .around(closeables);

  @Mock private StreamProcessor streamProcessor;
  @Mock private LogStreamReader logStreamReader;
  @Mock private TypedStreamWriter logStreamWriter;
  @Mock private CommandResponseWriter commandResponseWriter;
  @Mock private DbContext dbContext;
  @Mock private LogStream logStream;
  @Mock private TypedRecordProcessor mockRecordProcessor;

  private ZeebeDbTransaction zeebeDbTransaction;
  private ActorControl actor;

  @Before
  public void setup() {
    initMocks(this);

    streams =
        new TestStreams(
            tempFolder, closeables, serviceContainerRule.get(), actorSchedulerRule.get());
    commandResponseWriter = streams.getMockedResponseWriter();
    final LogStream stream = streams.createLogStream("stream");

    final ControllableActor controllableActor = new ControllableActor();
    actor = controllableActor.getActor();

    when(logStreamReader.hasNext()).thenReturn(true, false);
    when(logStreamReader.next()).thenReturn(mock(LoggedEvent.class));

    //    when(logStreamWriter.producerId(anyInt())).thenReturn(logStreamWriter);

    zeebeDbTransaction = mock(ZeebeDbTransaction.class);

    zeebeDbTransaction = spy(new Transaction());
    when(dbContext.getCurrentTransaction()).thenReturn(zeebeDbTransaction);
    when(logStream.getCommitPosition()).thenReturn(Long.MAX_VALUE);

    final RecordProcessorMap recordProcessorMap = mock(RecordProcessorMap.class);

    final TypedRecordProcessor mockRecordProcessor = mock(TypedRecordProcessor.class);
    when(recordProcessorMap.get(any(), any(), anyInt())).thenReturn(mockRecordProcessor);

    final ProcessingContext processingContext =
        new ProcessingContext()
            .logStream(logStream)
            .actor(actor)
            .logStreamReader(logStreamReader)
            .logStreamWriter(logStreamWriter)
            .streamProcessorName("testProcessor")
            .dbContext(dbContext)
            .recordProcessorMap(recordProcessorMap)
            .abortCondition(() -> false);

    processingStateMachine =
        new ProcessingStateMachine(
            processingContext, mock(StreamProcessorMetrics.class), () -> true);

    actorSchedulerRule.submitActor(controllableActor);
  }

  //  @Test
  //  public void shouldRunLifecycle() throws Exception {
  //    // given
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    doAnswer(
  //            (invocationOnMock) -> {
  //              latch.countDown();
  //              return invocationOnMock.callRealMethod();
  //            })
  //        .when(mockRecordProcessor)
  //        .processRecord(any(), any(), any());
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(
  //            logStreamReader,
  //            mockRecordProcessor,
  //            dbContext,
  //            zeebeDbTransaction,
  //            logStreamWriter,
  //            commandResponseWriter);
  //
  //    // process
  //    inOrder.verify(logStreamReader, times(1)).next();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).processRecord(any(), any(), any());
  //
  //    // write event
  //    inOrder.verify(logStreamWriter, times(1)).flush();
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    //    inOrder.verify(commandResponseWriter, times(1)).();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldRunLifecycleOnErrorInProcess() throws Exception {
  //    // given
  //    final RuntimeException expected = new RuntimeException("expected");
  //    doThrow(expected).doCallRealMethod().when(zeebeDbTransaction).run(any());
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              return true;
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //
  //    // on error
  //    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).onError(expected);
  //
  //    // write event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldNotContinueWhenCommitPositionIsSmallerThenErrorPosition() throws Exception {
  //    // given
  //    final RuntimeException expected = new RuntimeException("expected");
  //    doThrow(expected).doCallRealMethod().when(zeebeDbTransaction).run(any());
  //    final long errorEventPos = 1234L;
  //    when(mockRecordProcessor.writeEvent(any())).thenReturn(errorEventPos);
  //    when(logStreamReader.hasNext()).thenReturn(true);
  //    when(logStream.getCommitPosition()).thenReturn(1L);
  //
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              return true;
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //
  //    // on error
  //    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).onError(expected);
  //
  //    // write error event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldContinueWhenCommitPositionIsGreaterThenErrorPosition() throws Exception {
  //    // given
  //    final RuntimeException expected = new RuntimeException("expected");
  //    doThrow(expected).doCallRealMethod().when(zeebeDbTransaction).run(any());
  //    final long errorEventPos = 1234L;
  //    when(mockRecordProcessor.writeEvent(any())).thenReturn(errorEventPos);
  //    when(logStreamReader.hasNext()).thenReturn(true, true, false);
  //    when(logStream.getCommitPosition()).thenReturn(errorEventPos + 1);
  //
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              return true;
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //
  //    // on error
  //    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).onError(expected);
  //
  //    // write error event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //
  //    // == next iteration
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //
  //    // write error event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldRunLifecycleOnErrorInWriteEvent() throws Exception {
  //    // given
  //    final RuntimeException expected = new RuntimeException("expected");
  //    doThrow(expected).doReturn(1L).when(mockRecordProcessor).writeEvent(any());
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              return true;
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).processEvent();
  //
  //    // write event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // on error
  //    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).onError(expected);
  //
  //    // write event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldRunLifecycleOnErrorInUpdateState() throws Exception {
  //    // given
  //    final RuntimeException expected = new RuntimeException("expected");
  //    doThrow(expected).doNothing().when(zeebeDbTransaction).commit();
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              return true;
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).processEvent();
  //
  //    // write event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // on error
  //    inOrder.verify(zeebeDbTransaction, times(1)).rollback();
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).onError(expected);
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }
  //
  //  @Test
  //  public void shouldRunLifecycleOnErrorInExecuteSideEffects() throws Exception {
  //    // given
  //    final CountDownLatch latch = new CountDownLatch(1);
  //    when(mockRecordProcessor.executeSideEffects())
  //        .then(
  //            (invocationOnMock -> {
  //              latch.countDown();
  //              throw new RuntimeException("expected");
  //            }));
  //
  //    // when
  //    actor.call(() -> processingStateMachine.readNextEvent());
  //    actorSchedulerRule.workUntilDone();
  //
  //    // then
  //    latch.await();
  //    final InOrder inOrder =
  //        Mockito.inOrder(streamProcessor, mockRecordProcessor, dbContext, zeebeDbTransaction);
  //
  //    // process
  //    inOrder.verify(streamProcessor, times(1)).shouldProcess(any());
  //    inOrder.verify(dbContext, times(1)).getCurrentTransaction();
  //    inOrder.verify(zeebeDbTransaction, times(1)).run(any());
  //    inOrder.verify(mockRecordProcessor, times(1)).processEvent();
  //
  //    // write event
  //    inOrder.verify(mockRecordProcessor, times(1)).writeEvent(any());
  //
  //    // update state
  //    inOrder.verify(zeebeDbTransaction, times(1)).commit();
  //
  //    // execute side effects
  //    inOrder.verify(mockRecordProcessor, times(1)).executeSideEffects();
  //    inOrder.verifyNoMoreInteractions();
  //  }

  private class ControllableActor extends Actor {

    public ActorControl getActor() {
      return actor;
    }
  }

  private class Transaction implements ZeebeDbTransaction {

    @Override
    public void run(TransactionOperation operations) {
      try {
        operations.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void commit() {}

    @Override
    public void rollback() {}
  }
}
