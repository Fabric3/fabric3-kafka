package org.fabric3.binding.kafka.runtime;

import org.fabric3.api.annotation.wire.Key;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.container.builder.component.TargetConnectionAttacher;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.model.physical.PhysicalConnectionSource;

/**
 *
 */
@Key("org.fabric3.binding.kafka.provision.KafkaConnectionTarget")
public class KafkaConnectionSourceAttacher implements TargetConnectionAttacher<KafkaConnectionTarget> {

    public void attach(PhysicalConnectionSource source, KafkaConnectionTarget target, ChannelConnection connection) {

    }

    public void detach(PhysicalConnectionSource physicalConnectionSource, KafkaConnectionTarget kafkaConnectionTarget) {

    }
}
