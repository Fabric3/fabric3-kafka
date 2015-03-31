package org.fabric3.binding.kafka.runtime;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.fabric3.api.MonitorChannel;
import org.fabric3.api.annotation.monitor.Monitor;
import org.fabric3.api.host.Fabric3Exception;
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
    private static final List<Class<?>> TYPES = Arrays.asList(Producer.class, Consumer.class, ConsumerConnector.class);

    private Map<URI, Holder<Producer>> producers = new HashMap<>();
    private Map<URI, Map<URI, ConsumerConnector>> connectors = new HashMap<>();

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
        for (Map<URI, ConsumerConnector> holder : connectors.values()) {
            holder.values().forEach(ConsumerConnector::shutdown);
        }
    }

    public List<Class<?>> getTypes() {
        return TYPES;
    }

    public Producer<?, ?> getProducer(KafkaConnectionTarget target) {
        return producers.computeIfAbsent(target.getChannelUri(), (s) -> createProducer(target)).delegate;
    }

    public void releaseProducer(URI channelUri) {
        doRelease(channelUri, true);
    }

    public <T> T createDirectConsumer(Class<T> type, KafkaConnectionSource source) {
        if (ConsumerConnector.class.isAssignableFrom(type)) {
            Map<URI, ConsumerConnector> map = connectors.computeIfAbsent(source.getChannelUri(), (s) -> new HashMap());
            return type.cast(map.computeIfAbsent(source.getConsumerUri(), (s) -> createConsumerConnector(source)));
        } else {
            throw new Fabric3Exception("Invalid consumer type: " + type.getName());
        }
    }

    public void releaseConsumer(KafkaConnectionSource source) {
        URI channelUri = source.getChannelUri();
        URI consumerUri = source.getConsumerUri();
        doRelease(channelUri, consumerUri, true);
    }

    @SuppressWarnings("unchecked")
    public <T> Supplier<T> getConnection(URI channelUri, URI attachUri, Class<T> type) {
        // use the release
        if (Producer.class.isAssignableFrom(type)) {
            return () -> (T) doRelease(channelUri, false);
        } else if (ConsumerConnector.class.isAssignableFrom(type)) {
            return () -> (T) doRelease(channelUri, attachUri, false);
        } else {
            throw new Fabric3Exception("Invalid connection type: " + type.getName());
        }
    }

    public void subscribe(KafkaConnectionSource source, ChannelConnection connection) {
        String topic = source.getTopic() != null ? source.getTopic() : source.getDefaultTopic();
        Map<URI, ConsumerConnector> map = connectors.computeIfAbsent(source.getChannelUri(), (s) -> new HashMap());
        ConsumerConnector consumer = map.computeIfAbsent(source.getConsumerUri(), (s) -> createConsumerConnector(source));

        ConsumerIterator iterator = consumer.createMessageStreams(Collections.singletonMap(topic, 1)).get(topic).get(0).iterator();
        executorService.submit(() -> {
            while (iterator.hasNext()) {
                connection.getEventStream().getHeadHandler().handle(String.valueOf(iterator.next()), false);
            }
        });
        // set the closeable callback
        connection.getEventStream().setCloseable(() -> releaseConsumer(source));
    }

    public <T> Supplier<T> getConnection(URI uri, URI attachUri, Class<T> type, String topic) {
        return getConnection(uri, attachUri, type);
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
    private ConsumerConnector createConsumerConnector(KafkaConnectionSource source) {
        Properties properties = new Properties();
        properties.putAll(source.getConfiguration());
        ConsumerConfig config = new ConsumerConfig(properties);
        return kafka.consumer.Consumer.createJavaConsumerConnector(config);
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

    private ConsumerConnector doRelease(URI channelUri, URI consumerUri, boolean shutdown) {
        Map<URI, ConsumerConnector> connectorMap = connectors.get(channelUri);
        if (connectorMap == null) {
            return null;
        }
        ConsumerConnector connector = connectorMap.get(consumerUri);
        if (connector == null) {
            return null;
        }
        if (shutdown) {
            connector.shutdown();
        }
        connectors.remove(channelUri);
        return connector;
    }

    private Producer<?, ?> doRelease(URI channelUri, boolean shutdown) {
        Holder<Producer> holder = producers.get(channelUri);
        if (holder == null) {
            return null;
        }
        if (--holder.counter == 0) {
            if (shutdown) {
                holder.delegate.close();
            }
            producers.remove(channelUri);
        }
        return holder.delegate;
    }

    private class Holder<T> {

        public Holder(T delegate) {
            this.delegate = delegate;
        }

        T delegate;
        int counter = 1;
    }
}
