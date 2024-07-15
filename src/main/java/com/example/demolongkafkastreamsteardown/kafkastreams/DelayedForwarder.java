package com.example.demolongkafkastreamsteardown.kafkastreams;

import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
@Slf4j
public class DelayedForwarder extends AbstractProcessor<String, TestData> {

    private KeyValueStore<String, TestData> store;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);

        store = context().getStateStore("OPEN-TEST-DATA");
        initializeNotificationReportScheduler(context);
    }

    private void initializeNotificationReportScheduler(ProcessorContext context) {
        context.schedule(Duration.ofMillis(1_000), PunctuationType.WALL_CLOCK_TIME, timestamp ->
            {
                try (KeyValueIterator<String, TestData> openNotificationsStoreIterator = store.all()) {
                    openNotificationsStoreIterator.forEachRemaining(entry -> {
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        context.forward(entry.key, entry.value);
                        context().commit();
                    });
                }
            }
        );
    }

    @Override
    public void process(String key, TestData value) {
        TestData testData = new TestData(value.value(), Instant.now().plusMillis(100));
        store.put(key, testData);
    }
}
