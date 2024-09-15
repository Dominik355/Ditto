package com.bilik.ditto.testCommons;

import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventProducer;

import java.util.ArrayList;
import java.util.List;

public class MockingWorkerEventProducer implements WorkerEventProducer {

    private List<WorkerEvent> events = new ArrayList<>();

    @Override
    public void sendEvent(WorkerEvent event) {
        events.add(event);
    }

    public List<WorkerEvent> events() {
        return events;
    }
}