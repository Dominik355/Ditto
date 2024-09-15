package com.bilik.ditto.testCommons;

import com.bilik.ditto.core.common.Stoppable;

public class StoppableClass implements Stoppable {

    private boolean hasBeenStopped;

    @Override
    public void stop() throws Exception {
        hasBeenStopped = true;
    }

    public boolean hasBeenStopped() {
        return hasBeenStopped;
    }
}