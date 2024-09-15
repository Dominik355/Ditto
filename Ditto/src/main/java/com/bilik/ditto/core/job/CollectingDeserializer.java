package com.bilik.ditto.core.job;

import com.bilik.ditto.core.type.TypeSerde;

import java.io.IOException;

/**
 * Instead of returning the value, adds it to collector
 * @param <IN> - input value, probably byte[] or InputStream or something
 * @param <OUT> - deserialized output value like protobuf, avro, json ...
 */
public interface CollectingDeserializer<IN, OUT> {
    void deserialize(IN input, Collector<OUT> collector, long timestamp) throws InterruptedException;

    public static class DefaultCollectingDeserializer<OUT> implements CollectingDeserializer<byte[], OUT> {

        private final TypeSerde.Deserializer<OUT> deserializer;

        public DefaultCollectingDeserializer(TypeSerde.Deserializer<OUT> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public void deserialize(byte[] input, Collector<OUT> collector, long timestamp) throws InterruptedException {
            try {
                collector.collect(StreamElement.of(deserializer.deserialize(input), timestamp));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}