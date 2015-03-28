package org.fabric3.binding.kafka.provision;

import java.net.URI;
import java.util.Map;

import org.fabric3.spi.model.physical.PhysicalConnectionSource;

/**
 *
 */
public class KafkaConnectionSource extends PhysicalConnectionSource {
    private URI channelUri;
    private Map<String, Object> configuration;

    public KafkaConnectionSource(URI channelUri,Map<String, Object> configuration) {
        this.channelUri = channelUri;
        this.configuration = configuration;
    }

    public URI getChannelUri() {
        return channelUri;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }
}
