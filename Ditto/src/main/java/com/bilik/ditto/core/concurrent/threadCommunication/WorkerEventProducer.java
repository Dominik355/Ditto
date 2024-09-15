package com.bilik.ditto.core.concurrent.threadCommunication;

public interface WorkerEventProducer {

    /**
     * Sends event or throws exception if something goes worong
     */
    void sendEvent(WorkerEvent event);

    static WorkerEventProducer dummy() {
        return event -> {};
    }

}
