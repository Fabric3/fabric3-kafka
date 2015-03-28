package org.fabric3.binding.kafka.provision;

import java.net.URI;
import java.util.Map;

import org.fabric3.spi.model.physical.PhysicalConnectionSource;

/**
 *
 */
public class KafkaConnectionSource extends PhysicalConnectionSource {
    private URI channelUri;
    private String defaultTopic;

    private Map<String, Object> configuration;

    public KafkaConnectionSource(URI channelUri, String defaultTopic, Map<String, Object> configuration) {
        this.channelUri = channelUri;
        this.defaultTopic = defaultTopic;
        this.configuration = configuration;
    }

    public URI getChannelUri() {
        return channelUri;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }
}
