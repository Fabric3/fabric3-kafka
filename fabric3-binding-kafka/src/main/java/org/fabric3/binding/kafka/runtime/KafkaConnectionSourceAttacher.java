package org.fabric3.binding.kafka.runtime;

import org.fabric3.api.annotation.wire.Key;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.spi.container.builder.component.SourceConnectionAttacher;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.model.physical.PhysicalConnectionTarget;
import org.oasisopen.sca.annotation.Reference;

/**
 *
 */
@Key("org.fabric3.binding.kafka.provision.KafkaConnectionSource")
public class KafkaConnectionSourceAttacher implements SourceConnectionAttacher<KafkaConnectionSource> {

    @Reference
    protected KafkaConnectionManager connectionManager;

    public void attach(KafkaConnectionSource source, PhysicalConnectionTarget target, ChannelConnection connection) {
        if (target.isDirectConnection()) {
            Class<?> type = target.getServiceInterface();
            connectionManager.createDirectConsumer(type, source); // create consumer
        } else {
            connectionManager.subscribe(source, connection);
            connection.getEventStream().setCloseable(() -> connectionManager.releaseConsumer(source));
        }
    }

    public void detach(KafkaConnectionSource source, PhysicalConnectionTarget target) {
        connectionManager.releaseConsumer(source);
    }
}
