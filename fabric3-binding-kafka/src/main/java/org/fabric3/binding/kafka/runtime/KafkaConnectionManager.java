package org.fabric3.binding.kafka.runtime;

import java.net.URI;

import org.apache.kafka.clients.producer.Producer;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.channel.ChannelConnection;

/**
 * Manages producer and consumer connections to a Kafka cluster.
 */
public interface KafkaConnectionManager {

    /**
     * Returns a producer connection for the target configuration.
     *
     * @param target the target configuration
     * @return the producer
     */
    Producer<?, ?> getProducer(KafkaConnectionTarget target);

    /**
     * Closes a producer and releases resources used by it.
     *
     * @param channelUri the channel the producer is connected to
     */
    void releaseProducer(URI channelUri);

    /**
     * Creates a direct consumer connection.
     *
     * @param type   the connection type
     * @param source the source configuration
     * @return the connection
     */
    <T> T createDirectConsumer(Class<T> type, KafkaConnectionSource source);

    /**
     * Closes a consumer connection and resources used by it.
     *
     * @param source the source configuration
     */
    void releaseConsumer(KafkaConnectionSource source);

    /**
     * Subscribes the channel connection to a Kafka topic.
     *
     * @param source the source configuration
     */
    void subscribe(KafkaConnectionSource source, ChannelConnection connection);

}
