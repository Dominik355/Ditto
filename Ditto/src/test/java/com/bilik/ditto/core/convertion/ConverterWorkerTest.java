package com.bilik.ditto.core.convertion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Message;
import com.bilik.ditto.core.common.LoggingUncaughtExceptionHandler;
import com.bilik.ditto.core.common.StoppingUncaughtExceptionHandler;
import com.bilik.ditto.core.concurrent.WorkerThread;
import com.bilik.ditto.core.concurrent.WorkerThreadFactory;
import com.bilik.ditto.core.concurrent.threadCommunication.QueueWorkerEventCommunicator;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEvent;
import com.bilik.ditto.core.concurrent.threadCommunication.WorkerEventCommunicator;
import com.bilik.ditto.core.convertion.factory.ProtoToJsonConverterFactory;
import com.bilik.ditto.core.job.StreamElement;
import com.bilik.ditto.testCommons.SensorGenerator;
import com.bilik.ditto.testCommons.StoppableClass;
import org.junit.jupiter.api.Test;
import proto.test.Sensor;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.bilik.ditto.testCommons.SensorUtils.assertJsonFromProto;
import static com.bilik.ditto.testCommons.ThreadUtils.assertThreadTermination;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ConverterWorkers are being tested as separated threads, not only their run() method.
 */
public class ConverterWorkerTest {

    @Test
    void converterWorker_proto_to_json() throws InterruptedException, JsonProcessingException {
        // given
        int waitTimeMillis = 5_000;
        BlockingQueue<StreamElement<Message>> input = new LinkedBlockingQueue<>();
        BlockingQueue<StreamElement<String>> output = new LinkedBlockingQueue<>();
        WorkerEventCommunicator communicator = new QueueWorkerEventCommunicator(waitTimeMillis, TimeUnit.MILLISECONDS);
        Converter<Message, String> converter = new ProtoToJsonConverterFactory.ProtoToJsonConverter();
        ConverterWorker<Message, String> converterWorker = new ConverterWorker<>(communicator, input, output, converter);

        List<Sensor> data = SensorGenerator.getRange(0, 10);

        // when
        for(Sensor sensor : data) {
            input.offer(StreamElement.of(sensor));
        }

        assertThat(converterWorker.initThread()).isTrue();
        converterWorker.start();

        WorkerEvent startedEvent = communicator.receiveEvent();
        assertThat(startedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(startedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(startedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        for(int i = 0; i < 10; i++) {
            StreamElement<String> jsonElement = output.poll(waitTimeMillis, TimeUnit.MILLISECONDS);
            assertJsonFromProto(data.get(i), jsonElement.value());
        }

        converterWorker.shutdown();

        WorkerEvent finishedEvent = communicator.receiveEvent();
        assertThat(finishedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(finishedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(finishedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);
        assertThat(output.size()).isZero(); // no more than supplied events should be produced

        assertThreadTermination(converterWorker);
    }

    @Test
    void converterWorker_proto_to_json_with_factory() throws InterruptedException, JsonProcessingException {
        // given
        int waitTimeMillis = 5_000;
        BlockingQueue<StreamElement<Message>> input = new LinkedBlockingQueue<>();
        BlockingQueue<StreamElement<String>> output = new LinkedBlockingQueue<>();
        WorkerEventCommunicator communicator = new QueueWorkerEventCommunicator(waitTimeMillis, TimeUnit.MILLISECONDS);
        Converter<Message, String> converter = new ProtoToJsonConverterFactory.ProtoToJsonConverter();

        WorkerThreadFactory<ConverterWorker<Message, String>> converterThreadFactory =
                new WorkerThreadFactory<>("jobId-test",
                        LoggingUncaughtExceptionHandler.withChild(
                                new StoppingUncaughtExceptionHandler(new StoppableClass())),
                        WorkerThread.WorkerType.CONVERTER);

        ConverterWorker<Message, String> converterWorker = converterThreadFactory.modifyThread(
                new ConverterWorker<>(communicator, input, output, converter),
                23
        );

        // when
        assertThat(converterWorker.initThread()).isTrue();
        converterWorker.start();

        WorkerEvent startedEvent = communicator.receiveEvent();
        assertThat(startedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(startedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(startedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        List<Sensor> data = SensorGenerator.getRange(0, 10);

        for(Sensor sensor : data) {
            input.offer(StreamElement.of(sensor));
        }

        for(int i = 0; i < 10; i++) {
            StreamElement<String> jsonElement = output.poll(waitTimeMillis, TimeUnit.MILLISECONDS);
            assertJsonFromProto(data.get(i), jsonElement.value());
        }

        converterWorker.shutdown();

        WorkerEvent finishedEvent = communicator.receiveEvent();
        assertThat(finishedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(finishedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(finishedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);
        assertThat(output.size()).isZero(); // no more than supplied events should be produced

        assertThreadTermination(converterWorker);
    }

    @Test
    void converterWorker_proto_to_json_lastElement() throws InterruptedException, JsonProcessingException {
        // given
        int waitTimeMillis = 5_000;
        BlockingQueue<StreamElement<Message>> input = new LinkedBlockingQueue<>();
        BlockingQueue<StreamElement<String>> output = new LinkedBlockingQueue<>();
        WorkerEventCommunicator communicator = new QueueWorkerEventCommunicator(waitTimeMillis, TimeUnit.MILLISECONDS);
        Converter<Message, String> converter = new ProtoToJsonConverterFactory.ProtoToJsonConverter();
        ConverterWorker<Message, String> converterWorker = new ConverterWorker<>(communicator, input, output, converter);

        // when
        assertThat(converterWorker.initThread()).isTrue();
        converterWorker.start();

        WorkerEvent startedEvent = communicator.receiveEvent();
        assertThat(startedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(startedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(startedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.STARTED);

        List<Sensor> data = SensorGenerator.getRange(0, 10);

        for(Sensor sensor : data) {
            input.offer(StreamElement.of(sensor));
        }

        for(int i = 0; i < 10; i++) {
            StreamElement<String> jsonElement = output.poll(waitTimeMillis, TimeUnit.MILLISECONDS);
            assertJsonFromProto(data.get(i), jsonElement.value());
        }

        // last element
        input.offer(StreamElement.last());

        // should end thread without explicitly calling shutdown
        WorkerEvent finishedEvent = communicator.receiveEvent();
        assertThat(finishedEvent.getType()).isEqualTo(converterWorker.getType());
        assertThat(finishedEvent.getThreadNum()).isEqualTo(converterWorker.getWorkerNumber());
        assertThat(finishedEvent.getWorkerState()).isEqualTo(WorkerEvent.WorkerState.FINISHED);
        assertThat(output.poll(waitTimeMillis, TimeUnit.MILLISECONDS)).isEqualTo(StreamElement.last());
        assertThat(output.size()).isZero(); // no more than supplied events should be produced

        assertThreadTermination(converterWorker);
    }

}
