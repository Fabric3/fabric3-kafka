package org.fabric3.binding.kafka.generator;

import org.fabric3.api.annotation.wire.Key;
import org.fabric3.api.binding.kafka.model.KafkaBinding;
import org.fabric3.binding.kafka.provision.KafkaConnectionSource;
import org.fabric3.binding.kafka.provision.KafkaConnectionTarget;
import org.fabric3.spi.domain.generator.channel.ConnectionBindingGenerator;
import org.fabric3.spi.model.instance.LogicalBinding;
import org.fabric3.spi.model.instance.LogicalConsumer;
import org.fabric3.spi.model.instance.LogicalProducer;
import org.fabric3.spi.model.physical.DeliveryType;
import org.fabric3.spi.model.physical.PhysicalConnectionSource;
import org.fabric3.spi.model.physical.PhysicalConnectionTarget;
import org.oasisopen.sca.annotation.EagerInit;

/**
 *
 */
@EagerInit
@Key("org.fabric3.api.binding.kafka.model.KafkaBinding")
public class KafkaConnectionBindingGenerator implements ConnectionBindingGenerator<KafkaBinding> {
    public PhysicalConnectionSource generateConnectionSource(LogicalConsumer logicalConsumer,
                                                             LogicalBinding<KafkaBinding> logicalBinding,
                                                             DeliveryType deliveryType) {
        return new KafkaConnectionSource();

    }

    public PhysicalConnectionTarget generateConnectionTarget(LogicalProducer logicalProducer,
                                                             LogicalBinding<KafkaBinding> logicalBinding,
                                                             DeliveryType deliveryType) {
        return new KafkaConnectionTarget();
    }
}
