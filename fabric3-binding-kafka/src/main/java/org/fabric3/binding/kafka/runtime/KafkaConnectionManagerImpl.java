package org.fabric3.binding.kafka.runtime;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.fabric3.api.MonitorChannel;
import org.fabric3.api.annotation.monitor.Monitor;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.component.DirectConnectionFactory;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.container.channel.EventStream;
import org.fabric3.spi.runtime.event.EventService;
import org.fabric3.spi.runtime.event.RuntimeStart;
import org.oasisopen.sca.annotation.Destroy;
import org.oasisopen.sca.annotation.EagerInit;
import org.oasisopen.sca.annotation.Init;
import org.oasisopen.sca.annotation.Reference;
import org.oasisopen.sca.annotation.Service;

/**
 *
 */
@EagerInit
@Service({KafkaConnectionManager.class, DirectConnectionFactory.class})
public class KafkaConnectionManagerImpl implements KafkaConnectionManager, DirectConnectionFactory {
    private Map<URI, Holder<Producer>> producers = new HashMap<>();
    private Map<URI, Holder<Consumer>> consumers = new HashMap<>();

    private AtomicBoolean active = new AtomicBoolean();

    @Reference
    protected EventService eventService;

    @Reference
    protected ExecutorService executorService;

    @Monitor
    protected MonitorChannel monitor;

    private List<Runnable> queuedSubscriptions = new ArrayList<>();

    @Init
    public void init() {
        eventService.subscribe(RuntimeStart.class, (event) -> {
            active.set(true);
            queuedSubscriptions.forEach(executorService::submit);
            queuedSubscriptions.clear();
        });
    }

    @Destroy
    public void destroy() {
        active.set(false);
        for (Holder<Producer> holder : producers.values()) {
            holder.delegate.close();
        }
        for (Holder<Consumer> holder : consumers.values()) {
            holder.delegate.close();
        }
    }

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
        return consumers.computeIfAbsent(source.getChannelUri(), (s) -> createConsumer(source)).delegate;
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

    public void subscribe(KafkaConnectionSource source, ChannelConnection connection) {
        Consumer consumer = getConsumer(source);
        consumer.subscribe(source.getDefaultTopic());

        EventStream stream = connection.getEventStream();

        if (active.get()) {
            executorService.submit(poller(consumer, stream));
        } else {
            queuedSubscriptions.add(poller(consumer, stream));
        }
    }

    public <T> Supplier<T> getConnection(URI uri, Class<T> type, String topic) {
        return getConnection(uri, type);
    }

    @SuppressWarnings("unchecked")
    private Holder<Producer> createProducer(KafkaConnectionTarget target) {
        return new Holder(new KafkaProducer<>(target.getConfiguration()));
    }

    @SuppressWarnings("unchecked")
    private Holder<Consumer> createConsumer(KafkaConnectionSource source) {
        return new Holder(new KafkaConsumer<>(source.getConfiguration()));
    }

    @SuppressWarnings("unchecked")
    private Runnable poller(Consumer consumer, EventStream stream) {
        return () -> {
            while (active.get()) {
                Map<String, ConsumerRecords> map = consumer.poll(0);
                for (ConsumerRecords entry : map.values()) {
                    List<ConsumerRecord> records = entry.records();
                    for (ConsumerRecord record : records) {
                        try {
                            stream.getHeadHandler().handle(record.value(), false);
                        } catch (Exception e) {
                            monitor.severe("Error reading Kafka message", e);
                        }
                    }
                }
            }
        };
    }

    private class Holder<T> {

        public Holder(T delegate) {
            this.delegate = delegate;
        }

        T delegate;
        int counter = 1;
    }
}
