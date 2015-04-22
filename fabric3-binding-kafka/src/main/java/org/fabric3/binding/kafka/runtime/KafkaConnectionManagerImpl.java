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
import java.util.function.Function;
import java.util.function.Supplier;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fabric3.api.host.Fabric3Exception;
import org.fabric3.api.host.runtime.HostInfo;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.DirectConnectionFactory;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.container.component.Component;
import org.fabric3.spi.container.component.ComponentManager;
import org.fabric3.spi.container.component.ScopedComponent;
import org.fabric3.spi.runtime.event.EventService;
import org.fabric3.spi.runtime.event.RuntimeStart;
import org.fabric3.spi.util.Cast;
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

    @Reference
    protected HostInfo info;

    @Reference
    protected ComponentManager cm;

    @Reference
    protected EventService eventService;

    @Reference
    protected ExecutorService executorService;

    private List<Runnable> queuedSubscriptions = new ArrayList<>();

    @Init
    public void init() {
        eventService.subscribe(RuntimeStart.class, (event) -> {
            queuedSubscriptions.forEach(executorService::submit);
            queuedSubscriptions.clear();
        });
    }

    @Destroy
    public void destroy() {
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
        releaseProducer(channelUri, true);
    }

    public <T> T createDirectConsumer(Class<T> type, KafkaConnectionSource source) {
        if (ConsumerConnector.class.isAssignableFrom(type)) {
            Map<URI, ConsumerConnector> map = connectors.computeIfAbsent(source.getChannelUri(), (s) -> new HashMap());
            return type.cast(map.computeIfAbsent(source.getConsumerUri(), (s) -> createConsumer(source)));
        } else {
            throw new Fabric3Exception("Invalid consumer type: " + type.getName());
        }
    }

    public void releaseConsumer(KafkaConnectionSource source) {
        URI channelUri = source.getChannelUri();
        URI consumerUri = source.getConsumerUri();
        releaseConsumer(channelUri, consumerUri, true);
    }

    @SuppressWarnings("unchecked")
    public void subscribe(KafkaConnectionSource source, ChannelConnection connection) {
        String topic = source.getTopic() != null ? source.getTopic() : source.getDefaultTopic();
        Map<URI, ConsumerConnector> map = connectors.computeIfAbsent(source.getChannelUri(), (s) -> new HashMap());
        ConsumerConnector consumer = map.computeIfAbsent(source.getConsumerUri(), (s) -> createConsumer(source));

        Decoder keyDecoder = createDeserializer(source.getKeyDeserializer());
        Decoder valueDecoder = createDeserializer(source.getValueDeserializer());

        Map<String, List<KafkaStream<?, ?>>> streams = consumer.createMessageStreams(Collections.singletonMap(topic, 1), keyDecoder, valueDecoder);
        ConsumerIterator iterator = streams.get(topic).get(0).iterator();
        executorService.submit(() -> {
            while (iterator.hasNext()) {
                connection.getEventStream().getHeadHandler().handle(String.valueOf(iterator.next()), false);
            }
        });
        // set the closeable callback
        connection.setCloseable(() -> releaseConsumer(source));
    }

    @SuppressWarnings("unchecked")
    public <T> Supplier<T> getConnection(URI channelUri, URI attachUri, Class<T> type) {
        // use the release methods since direct connections are not managed and the client is responsible for closing resources
        if (Producer.class.isAssignableFrom(type)) {
            return () -> (T) releaseProducer(channelUri, false);
        } else if (ConsumerConnector.class.isAssignableFrom(type)) {
            return () -> (T) releaseConsumer(channelUri, attachUri, false);
        } else {
            throw new Fabric3Exception("Invalid connection type: " + type.getName());
        }
    }

    public <T> Supplier<T> getConnection(URI uri, URI attachUri, Class<T> type, String topic) {
        return getConnection(uri, attachUri, type);
    }

    @SuppressWarnings("unchecked")
    private Holder<Producer> createProducer(KafkaConnectionTarget target) {
        Map<String, Object> configuration = target.getConfiguration();
        Serializer keySerializer = createSerializer(target.getKeySerializer());
        Serializer valueSerializer = createSerializer(target.getValueSerializer());
        return new Holder<>(new KafkaProducer<>(configuration, keySerializer, valueSerializer));
    }

    private Serializer createSerializer(String name) {
        return name == null ? new StringSerializer() : new SerializerWrapper(Cast.cast(getInstance(name)));
    }

    @SuppressWarnings("unchecked")
    private ConsumerConnector createConsumer(KafkaConnectionSource source) {
        Properties properties = new Properties();
        properties.putAll(source.getConfiguration());
        ConsumerConfig config = new ConsumerConfig(properties);
        return kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    private ConsumerConnector releaseConsumer(URI channelUri, URI consumerUri, boolean shutdown) {
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

    private Producer<?, ?> releaseProducer(URI channelUri, boolean shutdown) {
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

    private Decoder createDeserializer(String name) {
        return name == null ? new DefaultDecoder(null) : new DeserializerWrapper(Cast.cast(getInstance(name)));
    }

    private Function getInstance(String name) {
        URI serializerUri = URI.create(info.getDomain().toString() + "/" + name);
        Component component = cm.getComponent(serializerUri);
        if (component == null) {
            throw new Fabric3Exception("Component not found: " + name);
        }
        if (!(component instanceof ScopedComponent)) {
            throw new Fabric3Exception("Component must be a Java component: " + name);
        }
        ScopedComponent scopedComponent = (ScopedComponent) component;
        Object instance = scopedComponent.getInstance();
        if (!(instance instanceof Function)) {
            throw new Fabric3Exception("Serializer must implement: " + Function.class.getName());
        }
        return (Function) instance;
    }

    private class Holder<T> {

        public Holder(T delegate) {
            this.delegate = delegate;
        }

        T delegate;
        int counter = 1;
    }
}
