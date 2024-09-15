package com.bilik.ditto.core.concurrent.threadCommunication;

public interface WorkerEventReceiver {

    /**
     * Retrieves and removes the head of this queue,
     */
    public WorkerEvent receiveEvent() throws InterruptedException;

}
