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
    public PhysicalConnectionSource generateConnectionSource(LogicalConsumer consumer, LogicalBinding<KafkaBinding> binding, DeliveryType deliveryType) {
        KafkaBinding kafkaBinding = binding.getDefinition();
        return new KafkaConnectionSource(binding.getParent().getUri(), kafkaBinding.getDefaultTopic(), kafkaBinding.getConfiguration());

    }

    public PhysicalConnectionTarget generateConnectionTarget(LogicalProducer producer, LogicalBinding<KafkaBinding> binding, DeliveryType deliveryType) {
        KafkaBinding kafkaBinding = binding.getDefinition();
        return new KafkaConnectionTarget(binding.getParent().getUri(), kafkaBinding.getDefaultTopic(), kafkaBinding.getConfiguration());
    }
}
