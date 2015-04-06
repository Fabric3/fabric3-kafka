package org.fabric3.test.binding.kafka;

import java.util.Collections;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fabric3.api.ChannelContext;
import org.fabric3.api.annotation.Channel;
import org.fabric3.api.annotation.Consumer;
import org.fabric3.api.annotation.Producer;
import org.fabric3.api.implementation.junit.Fabric3Runner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(Fabric3Runner.class)
public class TestClient {

    @Producer(target = "KafkaChannel")
    protected org.apache.kafka.clients.producer.Producer producer;

    @Producer(target = "KafkaChannel")
    protected TestChannel channel;

    @Consumer(source = "KafkaChannel")
    protected ConsumerConnector consumer;

    @Channel("KafkaChannel")
    protected ChannelContext channelContext;

    @Consumer(source = "KafkaChannel")
    public void receive(String message) {
        System.out.println("Received!!!!!!!:" + message);
    }

    @Test
    public void testProduce() throws Exception {
        ConsumerIterator<byte[], byte[]> iterator = consumer.createMessageStreams(Collections.singletonMap("test", 1)).get("test").get(0).iterator();

        Object handle = channelContext.subscribe(String.class, "id", "test", (m) -> System.out.println("Subscription::::::::::::" + m));

        new Thread(() -> {
            while (iterator.hasNext()) {
                byte[] s = iterator.next().message();
                System.out.println(new String(s));
            }
        }).start();

        producer.send(new ProducerRecord<>("test", "hello")).get();

        channel.publish("hello2");
        Thread.sleep(10000);

        TestChannel closeableProducer = channelContext.getProducer(TestChannel.class);
        channelContext.close(closeableProducer);
        channelContext.close(handle);
        ConsumerConnector closeableConnector = channelContext.getConsumer(ConsumerConnector.class, "test");

        System.out.println("Done");
    }
}
