package org.fabric3.binding.kafka.provision;

import java.net.URI;
import java.util.Map;

import org.fabric3.spi.model.physical.PhysicalConnectionSource;

/**
 *
 */
public class KafkaConnectionSource extends PhysicalConnectionSource {
    private URI channelUri;
    private URI consumerUri;
    private String defaultTopic;
    private String keyDeserializer;
    private final String valueDeserializer;

    private Map<String, Object> configuration;

    public KafkaConnectionSource(URI uri,
                                 URI channelUri,
                                 URI consumerUri,
                                 String defaultTopic,
                                 String keyDeserializer,
                                 String valueDeserializer,
                                 Map<String, Object> configuration) {
        setUri(uri);
        this.channelUri = channelUri;
        this.consumerUri = consumerUri;
        this.defaultTopic = defaultTopic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.configuration = configuration;
    }

    public String getSourceId() {
        return channelUri + "_Kafka_source";
    }

    public URI getChannelUri() {
        return channelUri;
    }

    public URI getConsumerUri() {
        return consumerUri;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }
}
