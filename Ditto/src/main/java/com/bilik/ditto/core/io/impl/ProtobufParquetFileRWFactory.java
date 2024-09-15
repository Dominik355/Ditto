package com.bilik.ditto.core.io.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Message;
import com.bilik.ditto.core.io.FileRWInfo;
import com.bilik.ditto.core.io.FileReader;
import com.bilik.ditto.core.io.FileRWFactory;
import com.bilik.ditto.core.io.FileWriter;
import com.bilik.ditto.core.io.PathWrapper;
import com.bilik.ditto.core.util.ProtoUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.proto.ProtoParquetWriter;

/**
 * Implementation for reading/writing protobuf messages to/from Parquet files.
 */
public class ProtobufParquetFileRWFactory implements FileRWFactory<Message> {

    private final FileRWInfo<Message> info;

    public ProtobufParquetFileRWFactory(FileRWInfo<Message> info) {
        this.info = info;
    }

    @Override
    public FileReader<Message> buildFileReader(PathWrapper path) throws Exception {
        return new ProtobufParquetFileReader(path.getHadoopPath());
    }

    @Override
    public FileWriter<Message> buildFileWriter(PathWrapper path) throws Exception {
        return new ProtobufParquetFileWriter(path.getHadoopPath());
    }

    @Override
    public FileRWInfo<Message> getInfo() {
        return info;
    }

    protected class ProtobufParquetFileReader implements FileReader<Message> {

        private final ParquetReader<Message> reader;

        public ProtobufParquetFileReader(Path path) throws IOException {
            this.reader = ProtoParquetReader.<Message>builder(path).build();
        }

        @Override
        public Message next() throws IOException {
            return ProtoUtils.asMessage(reader.read());
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    protected class ProtobufParquetFileWriter implements FileWriter<Message> {

        private final ParquetWriter<Message> writer;
        private AtomicInteger written = new AtomicInteger();

        /*
        I thought there is way to use non classpath proto messages for writing,
        but there is no way to define own ProtoWriteSupport using only Descriptor
        (which ProtoWriteSupport constructor allows), because there is no
        ProtoWriter constructor, or builder which would let you do that...
         */
        public ProtobufParquetFileWriter(Path path) throws IOException {
            this.writer = ProtoParquetWriter.<Message>builder(path)
                    .withMessage(getInfo().clasz())
                    .build();
        }

        @Override
        public long size() throws IOException {
            return writer.getDataSize();
        }

        @Override
        public void write(Message element) throws IOException {
            writer.write(element);
            written.incrementAndGet();
        }

        @Override
        public long writtenElements() {
            return written.get();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

}