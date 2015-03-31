package org.fabric3.test.binding.kafka;

import org.fabric3.api.implementation.junit.Fabric3Runner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(Fabric3Runner.class)
public class TestClient {

//    @Producer(target = "KafkaChannel")
//    protected org.apache.kafka.clients.producer.Producer producer;
//
//    @Producer(target = "KafkaChannel")
//    protected TestChannel channel;
//
//    @Consumer(source = "KafkaChannel")
//    protected ConsumerConnector consumer;
//
//    @Channel("KafkaChannel")
//    protected ChannelContext channelContext;
//
//    @Consumer(source = "KafkaChannel")
//    public void receive(String message) {
//        System.out.println("Received!!!!!!!:" + message);
//    }
//
    @Test
    public void testProduce() throws Exception {
//        ConsumerIterator<byte[], byte[]> iterator = consumer.createMessageStreams(Collections.singletonMap("test", 1)).get("test").get(0).iterator();
//
//        new Thread(() -> {
//            while (iterator.hasNext()) {
//                byte[] s = iterator.next().message();
//                System.out.println(new String(s));
//            }
//        }).start();
//
//        producer.send(new ProducerRecord<>("test", "hello")).get();
//
//        channel.publish("hello2");
//        Thread.sleep(10000);
//
//        TestChannel closeableProducer = channelContext.getProducer(TestChannel.class);
//        channelContext.close(closeableProducer);
//        ConsumerConnector closeableConnector = channelContext.getConsumer(ConsumerConnector.class, "test");
//
//        System.out.println("Done");
    }
}
