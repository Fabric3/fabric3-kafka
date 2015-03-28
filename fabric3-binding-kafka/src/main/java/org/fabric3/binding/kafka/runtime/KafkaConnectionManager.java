package org.fabric3.binding.kafka.runtime;

import java.net.URI;

import org.apache.kafka.clients.producer.Producer;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;

/**
 *
 */
public interface KafkaConnectionManager {

    Producer<?, ?> getProducer(KafkaConnectionTarget target);

    void release(URI channelUri);

}
