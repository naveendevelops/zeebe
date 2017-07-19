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
package io.zeebe.broker.task.processor;

import static io.zeebe.protocol.clientapi.EventType.TASK_EVENT;
import static io.zeebe.test.util.BufferAssert.assertThatBuffer;
import static io.zeebe.util.StringUtil.getBytes;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskEventType;
import io.zeebe.broker.test.MockStreamProcessorController;
import io.zeebe.broker.test.WrittenEvent;
import io.zeebe.logstreams.impl.test.MockLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.util.time.ClockUtil;

public class TaskExpireLockStreamProcessorTest
{
    private static final int STREAM_PROCESSOR_ID = 2;
    private static final DirectBuffer TARGET_LOG_STREAM_TOPIC_NAME = wrapString("test-topic");
    private static final int TARGET_LOG_STREAM_PARTITION_ID = 3;
    private static final long INITIAL_POSITION = 10L;
    private static final int TARGET_LOG_STREAM_TERM = 3;

    private static final byte[] TASK_TYPE = getBytes("test-task");
    private static final DirectBuffer TASK_TYPE_BUFFER = new UnsafeBuffer(TASK_TYPE);

    private static final long LOCK_TIME = ClockUtil.getCurrentTimeInMillis();
    private static final Instant BEFORE_LOCK_TIME = Instant.ofEpochMilli(LOCK_TIME).minusSeconds(60);
    private static final Instant AFTER_LOCK_TIME = Instant.ofEpochMilli(LOCK_TIME).plusSeconds(60);

    private TaskExpireLockStreamProcessor streamProcessor;

    private LogStreamWriter mockLogStreamWriter;

    @Mock
    private LoggedEvent mockLoggedEvent;

    @Mock
    private LogStream mockTargetLogStream;

    private MockLogStreamReader mockTargetLogStreamReader;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public MockStreamProcessorController<TaskEvent> mockController = new MockStreamProcessorController<>(TaskEvent.class, event -> event
            .setType(TASK_TYPE_BUFFER)
            .setLockTime(LOCK_TIME)
            .setLockOwner(wrapString("owner")),
            TASK_EVENT,
            INITIAL_POSITION);

    @Before
    public void setup() throws InterruptedException, ExecutionException
    {
        mockTargetLogStreamReader = new MockLogStreamReader();

        MockitoAnnotations.initMocks(this);

        when(mockTargetLogStream.getTopicName()).thenReturn(TARGET_LOG_STREAM_TOPIC_NAME);
        when(mockTargetLogStream.getPartitionId()).thenReturn(TARGET_LOG_STREAM_PARTITION_ID);
        when(mockTargetLogStream.getTerm()).thenReturn(TARGET_LOG_STREAM_TERM);

        streamProcessor = new TaskExpireLockStreamProcessor();

        final StreamProcessorContext streamProcessorContext = new StreamProcessorContext();
        streamProcessorContext.setId(STREAM_PROCESSOR_ID);
        streamProcessorContext.setTargetStream(mockTargetLogStream);
        streamProcessorContext.setTargetLogStreamReader(mockTargetLogStreamReader);

        mockController.initStreamProcessor(streamProcessor, streamProcessorContext);

        mockLogStreamWriter = streamProcessorContext.getLogStreamWriter();
    }

    @After
    public void cleanUp()
    {
        ClockUtil.reset();
    }

    @Test
    public void shoudExpireLockIfAfterLockTime()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        final LoggedEvent lockedEvent = mockController.buildLoggedEvent(2L, event -> event
                .setEventType(TaskEventType.LOCKED));

        mockController.processEvent(lockedEvent);

        mockTargetLogStreamReader.addEvent(lockedEvent);

        // when
        streamProcessor.checkLockExpirationAsync();

        mockController.drainCommandQueue();

        // then
        final WrittenEvent<TaskEvent> lastWrittenEvent = mockController.getLastWrittenEvent();

        final TaskEvent taskEvent = lastWrittenEvent.getValue();
        assertThat(taskEvent.getEventType()).isEqualTo(TaskEventType.EXPIRE_LOCK);
        assertThatBuffer(taskEvent.getType()).hasBytes(TASK_TYPE_BUFFER);

        final BrokerEventMetadata metadata = lastWrittenEvent.getMetadata();
        assertThat(metadata.getProtocolVersion()).isEqualTo(Protocol.PROTOCOL_VERSION);
        assertThat(metadata.getEventType()).isEqualTo(TASK_EVENT);
        assertThat(metadata.getRaftTermId()).isEqualTo(TARGET_LOG_STREAM_TERM);

        verify(mockLogStreamWriter).key(2L);
        verify(mockLogStreamWriter).producerId(STREAM_PROCESSOR_ID);
        verify(mockLogStreamWriter).sourceEvent(TARGET_LOG_STREAM_TOPIC_NAME, TARGET_LOG_STREAM_PARTITION_ID, INITIAL_POSITION);
    }

    @Test
    public void shoudExpireLockOnlyOnce()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        final LoggedEvent lockedEvent = mockController.buildLoggedEvent(2L, event -> event
                .setEventType(TaskEventType.LOCKED));

        mockController.processEvent(lockedEvent);

        mockTargetLogStreamReader.addEvent(lockedEvent);

        // when
        streamProcessor.checkLockExpirationAsync();
        mockController.drainCommandQueue();

        streamProcessor.checkLockExpirationAsync();
        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).hasSize(1);
    }

    @Test
    public void shouldExpireMultipleLockedTasksAtOnce()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        final LoggedEvent event1 = mockController.buildLoggedEvent(2L, event -> event
                .setEventType(TaskEventType.LOCKED));
        final LoggedEvent event2 = mockController.buildLoggedEvent(3L, event -> event
                .setEventType(TaskEventType.LOCKED));

        mockTargetLogStreamReader.addEvent(event1);
        mockTargetLogStreamReader.addEvent(event2);

        mockController.processEvent(event1);
        mockController.processEvent(event2);

        // when
        streamProcessor.checkLockExpirationAsync();
        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).hasSize(2);
    }

    @Test
    public void shoudNotExpireLockIfBeforeLockTime()
    {
        // given
        ClockUtil.setCurrentTime(BEFORE_LOCK_TIME);

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.LOCKED));

        // when
        streamProcessor.checkLockExpirationAsync();

        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).isEmpty();
    }

    @Test
    public void shouldNotExpireLockIfCompleted()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.LOCKED));

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.COMPLETED));

        // when
        streamProcessor.checkLockExpirationAsync();

        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).isEmpty();
    }

    @Test
    public void shouldNotExpireLockIfMarkedAsFailed()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.LOCKED));

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.FAILED));

        // when
        streamProcessor.checkLockExpirationAsync();

        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).isEmpty();
    }

    @Test
    public void shouldNotExpireLockIfAlreadyExpired()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.LOCKED));

        mockController.processEvent(2L, event ->
            event.setEventType(TaskEventType.LOCK_EXPIRED));

        // when
        streamProcessor.checkLockExpirationAsync();

        mockController.drainCommandQueue();

        // then
        assertThat(mockController.getWrittenEvents()).isEmpty();
    }

    @Test
    public void shouldFailToExpireLockTimeIfEventNotFound()
    {
        // given
        ClockUtil.setCurrentTime(AFTER_LOCK_TIME);

        mockController.processEvent(2L, event -> event
                .setEventType(TaskEventType.LOCKED));

        // when
        streamProcessor.checkLockExpirationAsync();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Failed to check the task lock expiration time");

        mockController.drainCommandQueue();
    }

}
