package org.fabric3.binding.kafka.runtime;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.component.DirectConnectionFactory;
import org.oasisopen.sca.annotation.Service;

/**
 *
 */
@Service({KafkaConnectionManager.class, DirectConnectionFactory.class})
public class KafkaConnectionManagerImpl implements KafkaConnectionManager, DirectConnectionFactory {
    private Map<URI, Holder<Producer>> producers = new HashMap<>();
    private Map<URI, Holder<Consumer>> consumers = new HashMap<>();

    public Producer<?, ?> getProducer(KafkaConnectionTarget target) {
        return producers.computeIfAbsent(target.getChannelUri(), (s) -> createProducer(target)).delegate;
    }

    public void releaseProducer(URI channelUri) {
        Holder<Producer> holder = producers.get(channelUri);
        if (--holder.counter == 0) {
            holder.delegate.close();
            producers.remove(channelUri);
        }
    }

    public Consumer<?, ?> getConsumer(KafkaConnectionSource source) {
        throw new UnsupportedOperationException();
    }

    public void releaseConsumer(URI channelUri) {
        Holder<Consumer> holder = consumers.get(channelUri);
        if (--holder.counter == 0) {
            holder.delegate.close();
            producers.remove(channelUri);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Supplier<T> getConnection(URI uri, Class<T> type) {
        if (Producer.class.isAssignableFrom(type)) {
            return () -> (T) producers.get(uri).delegate;
        } else {
            return () -> (T) consumers.get(uri).delegate;
        }
    }

    public <T> Supplier<T> getConnection(URI uri, Class<T> type, String topic) {
        return getConnection(uri, type);
    }

    @SuppressWarnings("unchecked")
    private Holder<Producer> createProducer(KafkaConnectionTarget target) {
        return new Holder(new KafkaProducer<>(target.getConfiguration()));
    }

    private class Holder<T> {

        public Holder(T delegate) {
            this.delegate = delegate;
        }

        T delegate;
        int counter = 1;
    }
}
