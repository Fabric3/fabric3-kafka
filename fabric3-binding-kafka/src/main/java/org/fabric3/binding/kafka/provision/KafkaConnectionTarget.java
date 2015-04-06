package org.fabric3.binding.kafka.provision;

import java.net.URI;
import java.util.Map;

import org.fabric3.spi.model.physical.PhysicalConnectionTarget;

/**
 *
 */
public class KafkaConnectionTarget extends PhysicalConnectionTarget {
    private URI channelUri;
    private String defaultTopic;
    private final String keySerializer;
    private final String valueSerializer;
    private Map<String, Object> configuration;

    public KafkaConnectionTarget(URI uri,
                                 URI channelUri,
                                 String defaultTopic,
                                 String keySerializer,
                                 String valueSerializer,
                                 Map<String, Object> configuration) {
        setUri(uri);
        this.channelUri = channelUri;
        this.defaultTopic = defaultTopic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.configuration = configuration;
    }

    public URI getChannelUri() {
        return channelUri;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

}
