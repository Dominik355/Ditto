package com.bilik.ditto.core.io.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.FileReader;
import com.bilik.ditto.core.io.FileWriter;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.util.FileUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bilik.ditto.core.util.FileUtils.newCreateWriteChannel;

/**
 * Why FileChannel ? is it faster ? Should be for our use-case, not sure though,
 * but it's an easy way to get size of written data (not real file sile).
 */
public class JsonArrayFileRWFactory implements FileRWFactory<String> {

    private final FileRWInfo<String> info;

    public JsonArrayFileRWFactory(FileRWInfo<String> info) {
        this.info = info;
    }

    @Override
    public FileReader<String> buildFileReader(PathWrapper path) throws Exception {
        return new JsonArrayFileReader(path.getNioPath());
    }

    @Override
    public FileWriter<String> buildFileWriter(PathWrapper path) throws Exception {
        return new JsonArrayFileWriter(path.getNioPath());
    }

    @Override
    public FileRWInfo<String> getInfo() {
        return info;
    }

    protected class JsonArrayFileReader implements FileReader<String> {

        private final JsonParser jp;

        public JsonArrayFileReader(Path path) {
            try {
                JsonFactory factory = new MappingJsonFactory();
                jp = factory.createParser(path.toFile());

                if (jp.nextToken() != JsonToken.START_ARRAY) {
                    throw new IllegalStateException("File has to represent pure JSON array");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String next() throws IOException {
            if (jp.nextToken() == JsonToken.END_ARRAY) {
                return null;
            }

            TreeNode tn = jp.readValueAsTree();
            jp.skipChildren();
            return tn.toString();
        }

        @Override
        public void close() throws IOException {
            jp.close();
        }
        
    }

    protected class JsonArrayFileWriter implements FileWriter<String> {
        private static final String SEPARATOR = ",\n";
        private static final String ARRAY_START = "[\n";
        private static final String ARRAY_END = "\n]";

        private FileChannel fileChannel;
        private long lastSize;
        private boolean hasElement;

        private AtomicInteger written = new AtomicInteger();

        public JsonArrayFileWriter(Path path) {
            try {
                this.fileChannel = newCreateWriteChannel(path);
                internalWrite(ARRAY_START);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long size() throws IOException {
            if (!fileChannel.isOpen()) {
                return lastSize;
            }
            return lastSize = fileChannel.size();
        }

        @Override
        public void write(String element) throws IOException {
            if (hasElement) {
                element = SEPARATOR + element;
            }
            FileUtils.write(fileChannel, element);
            written.incrementAndGet();
            hasElement = true;
        }

        @Override
        public long writtenElements() {
            return written.get();
        }

        @Override
        public void close() throws IOException {
            internalWrite(ARRAY_END);
            fileChannel.force(true);
            fileChannel.close();
        }

        private void internalWrite(String string) throws IOException {
            FileUtils.write(fileChannel, string);
        }

    }

}
