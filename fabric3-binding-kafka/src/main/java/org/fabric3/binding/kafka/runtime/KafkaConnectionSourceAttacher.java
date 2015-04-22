package org.fabric3.binding.kafka.runtime;

import org.fabric3.api.annotation.wire.Key;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.spi.container.builder.SourceConnectionAttacher;
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
        connection.setCloseable(() -> connectionManager.releaseConsumer(source));
        if (target.isDirectConnection()) {
            Class<?> type = target.getServiceInterface();
            connectionManager.createDirectConsumer(type, source); // create consumer, which will be returned by the direct connection factory
        } else {
            connectionManager.subscribe(source, connection);
        }
    }

    public void detach(KafkaConnectionSource source, PhysicalConnectionTarget target) {
        connectionManager.releaseConsumer(source);
    }
}
