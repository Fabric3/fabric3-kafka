package org.fabric3.binding.kafka.runtime;

import java.net.URI;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.channel.ChannelConnection;

/**
 *
 */
public interface KafkaConnectionManager {

    Producer<?, ?> getProducer(KafkaConnectionTarget target);

    void releaseProducer(URI channelUri);

    Consumer<?, ?> getConsumer(KafkaConnectionSource source);

    void releaseConsumer(URI channelUri);

    void subscribe(KafkaConnectionSource source, ChannelConnection connection);
}
