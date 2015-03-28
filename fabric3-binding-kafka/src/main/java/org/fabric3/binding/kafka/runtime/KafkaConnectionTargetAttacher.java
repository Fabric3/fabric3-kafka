package org.fabric3.binding.kafka.runtime;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.fabric3.api.annotation.wire.Key;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.component.TargetConnectionAttacher;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.model.physical.PhysicalConnectionSource;
import org.oasisopen.sca.annotation.Reference;

/**
 *
 */
@Key("org.fabric3.binding.kafka.provision.KafkaConnectionTarget")
public class KafkaConnectionTargetAttacher implements TargetConnectionAttacher<KafkaConnectionTarget> {
    @Reference
    protected KafkaConnectionManager connectionManager;

    @SuppressWarnings("unchecked")
    public void attach(PhysicalConnectionSource source, KafkaConnectionTarget target, ChannelConnection connection) {
        Producer<?, ?> producer = connectionManager.getProducer(target);
        if (!target.isDirectConnection()) {
            connection.getEventStream().addHandler((event, batch) -> producer.send(new ProducerRecord(target.getDefaultTopic(), event)));
        }
    }

    public void detach(PhysicalConnectionSource source, KafkaConnectionTarget target) {
        connectionManager.releaseProducer(target.getChannelUri());
    }
}
