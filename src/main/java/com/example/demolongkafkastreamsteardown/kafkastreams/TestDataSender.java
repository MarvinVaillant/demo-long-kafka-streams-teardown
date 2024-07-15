package com.example.demolongkafkastreamsteardown.kafkastreams;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.springframework.web.client.RestTemplate;

public class TestDataSender extends AbstractProcessor<String, TestData> {


    private final RestTemplate restTemplate = new RestTemplate();

    private final String externalUrl;

    public TestDataSender(String externalUrl) {
        this.externalUrl = externalUrl;
    }

    @Override
    public void process(String key, TestData value) {

        restTemplate.postForEntity(externalUrl + "/testdata", value, TestData.class);
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        context.forward(key, value);
    }
}
