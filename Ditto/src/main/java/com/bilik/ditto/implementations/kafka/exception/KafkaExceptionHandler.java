package com.bilik.ditto.implementations.kafka.exception;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaExceptionHandler<K, V> {
    /**
     * Determine whether or not to continue processing, based on producerRecord and exception.
     */
    KafkaExceptionHandlerResponse handle(final ProducerRecord<K, V> record,
                                              final Exception exception);

    enum KafkaExceptionHandlerResponse {
        CONTINUE(0, "CONTINUE"),
        FAIL(1, "FAIL");

        /**
         * just a descriptor, not used in code
         */
        public final String name;
        public final int id;

        KafkaExceptionHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

    class AlwaysContinueKafkaExceptionHandler<K, V> implements KafkaExceptionHandler<K, V> {

        @Override
        public KafkaExceptionHandlerResponse handle(final ProducerRecord<K, V> record,
                                                    final Exception exception) {
            return KafkaExceptionHandlerResponse.CONTINUE;
        }

    }

    class AlwaysFailKafkaExceptionHandler<K, V> implements KafkaExceptionHandler<K, V> {

        @Override
        public KafkaExceptionHandlerResponse handle(final ProducerRecord<K, V> record,
                                                    final Exception exception) {
            return KafkaExceptionHandlerResponse.FAIL;
        }

    }
}