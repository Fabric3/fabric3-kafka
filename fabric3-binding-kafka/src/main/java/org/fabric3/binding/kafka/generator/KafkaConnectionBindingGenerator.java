package org.fabric3.binding.kafka.generator;

import java.net.URI;
import java.util.Map;

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
        URI channelUri = binding.getParent().getUri();
        URI consumerUri = consumer.getUri();
        String defaultTopic = kafkaBinding.getDefaultTopic();
        String keyDeserializer = kafkaBinding.getKeyDeserializer();
        String valueDeserializer = kafkaBinding.getValueDeserializer();
        Map<String, Object> configuration = kafkaBinding.getConfiguration();
        return new KafkaConnectionSource(consumerUri, channelUri, consumerUri, defaultTopic, keyDeserializer, valueDeserializer, configuration);

    }

    public PhysicalConnectionTarget generateConnectionTarget(LogicalProducer producer, LogicalBinding<KafkaBinding> binding, DeliveryType deliveryType) {
        KafkaBinding kafkaBinding = binding.getDefinition();
        String keySerializer = kafkaBinding.getKeySerializer();
        String valueSerializer = kafkaBinding.getValueSerializer();
        Map<String, Object> configuration = kafkaBinding.getConfiguration();
        URI channelUri = binding.getParent().getUri();
        String defaultTopic = kafkaBinding.getDefaultTopic();
        return new KafkaConnectionTarget(producer.getUri(), channelUri, defaultTopic, keySerializer, valueSerializer, configuration);
    }
}
