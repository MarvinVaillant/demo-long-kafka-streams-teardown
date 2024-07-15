package com.example.demolongkafkastreamsteardown.kafkastreams;

import static org.springframework.kafka.support.serializer.JsonDeserializer.USE_TYPE_INFO_HEADERS;
import static org.springframework.kafka.support.serializer.JsonDeserializer.VALUE_DEFAULT_TYPE;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
@Configuration
public class MyKafkaStreamsTask {

    public static final String SOURCE = "Source-Topic";
    public static final String SECOND_PROCESSOR = "Second-Processor";
    public static final String FIRST_PROCESSOR = "First-Processor";

    public static final String SINK = "Sink-Topic";

    @Value("${kafka.bootstrapServers}")
    private String kafkaBootstrapServers;

    @Value("${external.url}")
    private String externalUrl;

    @Bean
    public KafkaStreams occurrenceReporting() {
        var kafkaStreams = createKafkaStreams(createTopology());
        kafkaStreams.start();
        return kafkaStreams;
    }

    public Topology createTopology() {

        Topology topology = new Topology();

        JsonDeserializer<TestData> testDataJsonDeserializer = new JsonDeserializer<>(TestData.class);

        topology.addSource(SOURCE,
            new StringDeserializer(),
            testDataJsonDeserializer,
            "source-topic");

        topology.addProcessor(FIRST_PROCESSOR,
            DelayedForwarder::new,
            SOURCE
        );

        StringSerde stringSerde = new StringSerde();

        JsonSerde<TestData> testDataJsonSerde = new JsonSerde<>();
        testDataJsonSerde.configure(Map.of(VALUE_DEFAULT_TYPE, TestData.class.getName(), USE_TYPE_INFO_HEADERS, false), false);

        topology.addStateStore(
            new KeyValueStoreBuilder<>(
                Stores.inMemoryKeyValueStore("OPEN-TEST-DATA"),
                stringSerde,
                testDataJsonSerde,
                Time.SYSTEM
            ), FIRST_PROCESSOR);

        topology.addProcessor(SECOND_PROCESSOR,
            () -> new TestDataSender(externalUrl),
            FIRST_PROCESSOR);

        topology.addSink(SINK, "sink-topic",
            new StringSerializer(),
            new JsonSerializer<>(),
            SECOND_PROCESSOR);

        return topology;
    }

    public KafkaStreams createKafkaStreams(Topology topology) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        properties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TestData.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        properties.put(StreamsConfig.RETRIES_CONFIG, 5);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "our-test-app");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, String.format("/tmp/kafka-streams/%s", UUID.randomUUID()));

        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        properties.put(USE_TYPE_INFO_HEADERS, false);

        return new KafkaStreams(topology, properties);
    }

}
