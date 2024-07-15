package com.example.demolongkafkastreamsteardown;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

import com.example.demolongkafkastreamsteardown.kafkastreams.TestData;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.verification.FindRequestsResult;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
class DemoLongKafkaStreamsTeardownApplicationTests {

    private static final List<String> REQUIRED_TOPICS = List.of("source-topic", "sink-topic");

    @Container
    final static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    final static WireMockServer wireMockServer = new WireMockServer(new WireMockConfiguration().port(19933));

    static {
        wireMockServer.start();
    }

    private KafkaTemplate<String, TestData> producer;


    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("kafka.bootstrapServers", kafka::getBootstrapServers);
        registry.add("external.url", wireMockServer::baseUrl);
    }

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(configs)) {
            List<NewTopic> newTopics = REQUIRED_TOPICS.stream().map(topicName -> new NewTopic(topicName, 2, (short) 1))
                .toList();

            adminClient.createTopics(newTopics);
        }
    }

    @BeforeEach
    void setUp() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, TestData> producerFactory = new DefaultKafkaProducerFactory<>(
            props,
            new StringSerializer(),
            new JsonSerializer<TestData>()
        );

        var kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic("source-topic");
        producer = kafkaTemplate;
    }

    @Test
    void test() throws ExecutionException, InterruptedException {
        // GIVEN
        wireMockServer.addStubMapping(post(urlPathEqualTo("/testdata"))
            .willReturn(aResponse().withStatus(200))
            .build());

        for (int i = 0; i < 1_000; i++) {
            TestData testData = new TestData("test", Instant.now());

            CompletableFuture<SendResult<String, TestData>> send = producer.send("source-topic", "some-key", testData);
            send.get();
        }

        // WHEN

        // THEN
        Awaitility.await().untilAsserted(() -> {
            RequestPattern requestFilter = postRequestedFor(urlMatching("/testdata")).build();

            FindRequestsResult requestsMatching = wireMockServer.findRequestsMatching(requestFilter);
            assertThat(requestsMatching.getRequests()).hasSize(1_000);
        });
    }
}
