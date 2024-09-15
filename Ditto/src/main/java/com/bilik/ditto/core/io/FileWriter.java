package com.bilik.ditto.core.io;

import java.io.Closeable;
import java.io.IOException;

/**
 * Should be returned by a FileReaderWriterFactory that also builds a corresponding FileReader.
 */
public interface FileWriter<T> extends Closeable {

    /**
     * @return size of data written to the underlying file
     */
    long size() throws IOException;

    /**
     * Write the given element value to the file
     */
    void write(T element) throws IOException;

    /**
     * @return num of written elements
     */
    long writtenElements();

    /**
     * Close the file
     */
    void close() throws IOException;

}
