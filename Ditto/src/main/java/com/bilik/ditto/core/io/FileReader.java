package com.bilik.ditto.core.io;

import java.io.IOException;

public interface FileReader<T> extends AutoCloseable {
    /**
     * Get the next element from the file
     * @return the next record or null if finished
     * @throws IOException if there is an error while reading
     */
    public T next() throws IOException;

}
