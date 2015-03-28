package org.fabric3.binding.kafka.runtime;

import org.fabric3.api.annotation.wire.Key;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.spi.container.builder.component.SourceConnectionAttacher;
import org.fabric3.spi.container.channel.ChannelConnection;
import org.fabric3.spi.model.physical.PhysicalConnectionTarget;

/**
 *
 */
@Key("org.fabric3.binding.kafka.provision.KafkaConnectionSource")
public class KafkaConnectionTargetAttacher implements SourceConnectionAttacher<KafkaConnectionSource> {

    public void attach(KafkaConnectionSource source, PhysicalConnectionTarget target, ChannelConnection connection) {

    }

    public void detach(KafkaConnectionSource source, PhysicalConnectionTarget target) {

    }
}
