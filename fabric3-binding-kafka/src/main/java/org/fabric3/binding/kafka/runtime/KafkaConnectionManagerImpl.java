package org.fabric3.binding.kafka.runtime;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.component.DirectConnectionFactory;
import org.oasisopen.sca.annotation.Service;

/**
 *
 */
@Service({KafkaConnectionManager.class, DirectConnectionFactory.class})
public class KafkaConnectionManagerImpl implements KafkaConnectionManager, DirectConnectionFactory<Producer<?, ?>> {
    private Map<URI, Holder> producers = new HashMap<>();

    public Producer<?, ?> getProducer(KafkaConnectionTarget target) {
        return producers.computeIfAbsent(target.getChannelUri(), (s) -> createProducer(target)).producer;
    }

    public void release(URI channelUri) {
        Holder holder = producers.get(channelUri);
        if (--holder.counter == 0) {
            holder.producer.close();
            producers.remove(channelUri);
        }
    }

    public Supplier<Producer<?, ?>> getConnection(URI uri) {
        return () -> producers.get(uri).producer;
    }

    public Supplier<Producer<?, ?>> getConnection(URI uri, String s) {
        return getConnection(uri);
    }

    private Holder createProducer(KafkaConnectionTarget target) {
        return new Holder(new KafkaProducer<>(target.getConfiguration()));
    }

    private class Holder {

        public Holder(Producer<?, ?> producer) {
            this.producer = producer;
        }

        Producer<?, ?> producer;
        int counter = 1;
    }
}
