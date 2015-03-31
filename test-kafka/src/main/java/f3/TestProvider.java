package f3;

import javax.xml.namespace.QName;

import org.fabric3.api.annotation.model.Provides;
import org.fabric3.api.binding.kafka.model.KafkaBinding;
import org.fabric3.api.binding.kafka.model.KafkaBindingBuilder;
import org.fabric3.api.model.type.builder.ChannelBuilder;
import org.fabric3.api.model.type.builder.CompositeBuilder;
import org.fabric3.api.model.type.component.Composite;

/**
 *
 */
public class TestProvider {
    private static final QName QNAME = new QName("urn:org.fabric3", "TestComposite");

    @Provides
    public static Composite getComposite() {
        CompositeBuilder compositeBuilder = CompositeBuilder.newBuilder(QNAME);
        ChannelBuilder channelBuilder = ChannelBuilder.newBuilder("KafkaChannel");
        KafkaBindingBuilder bindingBuilder = KafkaBindingBuilder.newBuilder();
        bindingBuilder.defaultTopic("test");
        bindingBuilder.configuration("partition.assignment.strategy", "roundrobin");
        bindingBuilder.configuration("group.id", "test");
        bindingBuilder.configuration("zookeeper.connect","127.0.0.1:2181");
        bindingBuilder.configuration("metadata.broker.list", "127.0.0.1:9092");
        bindingBuilder.configuration("bootstrap.servers", "127.0.0.1:9092");
        bindingBuilder.configuration("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        bindingBuilder.configuration("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        bindingBuilder.configuration("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        bindingBuilder.configuration("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaBinding binding = bindingBuilder.build();
        channelBuilder.binding(binding);
        compositeBuilder.channel(channelBuilder.build());
        compositeBuilder.deployable();
        return compositeBuilder.build();
    }
}
