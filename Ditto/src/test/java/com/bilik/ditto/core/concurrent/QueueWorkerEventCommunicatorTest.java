package com.bilik.ditto.core.concurrent;

import com.bilik.ditto.core.concurrent.WorkerThread.WorkerType;
import com.bilik.ditto.core.concurrent.threadCommunication.QueueWorkerEventCommunicator;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent.WorkerState;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueWorkerEventCommunicatorTest {
    
    @Test
    void queueWorkerEventCommunicatorTest() throws InterruptedException {
        QueueWorkerEventCommunicator communicator = new QueueWorkerEventCommunicator(1, TimeUnit.MILLISECONDS);
        
        communicator.sendEvent(new WorkerEvent(WorkerState.STARTED, WorkerType.CONVERTER));
        communicator.sendEvent(new WorkerEvent(WorkerState.STARTED, WorkerType.SOURCE));
        communicator.sendEvent(new WorkerEvent(WorkerState.STARTED, WorkerType.SINK));
        communicator.sendEvent(new WorkerEvent(WorkerState.FINISHED, WorkerType.SOURCE));
        communicator.sendEvent(new WorkerEvent(WorkerState.ERROR, WorkerType.SINK));

        var event = communicator.receiveEvent();
        assertThat(event.getWorkerState()).isEqualTo(WorkerState.STARTED);
        assertThat(event.getType()).isEqualTo(WorkerType.CONVERTER);

        event = communicator.receiveEvent();
        assertThat(event.getWorkerState()).isEqualTo(WorkerState.STARTED);
        assertThat(event.getType()).isEqualTo(WorkerType.SOURCE);

        event = communicator.receiveEvent();
        assertThat(event.getWorkerState()).isEqualTo(WorkerState.STARTED);
        assertThat(event.getType()).isEqualTo(WorkerType.SINK);

        event = communicator.receiveEvent();
        assertThat(event.getWorkerState()).isEqualTo(WorkerState.FINISHED);
        assertThat(event.getType()).isEqualTo(WorkerType.SOURCE);

        event = communicator.receiveEvent();
        assertThat(event.getWorkerState()).isEqualTo(WorkerState.ERROR);
        assertThat(event.getType()).isEqualTo(WorkerType.SINK);

        assertThat(communicator.receiveEvent()).isNull();
    }
    
}
