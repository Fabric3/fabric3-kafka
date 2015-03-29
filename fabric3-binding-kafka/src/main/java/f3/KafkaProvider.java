package f3;

import javax.xml.namespace.QName;

import org.fabric3.api.Namespaces;
import org.fabric3.api.annotation.model.Provides;
import org.fabric3.api.model.type.builder.CompositeBuilder;
import org.fabric3.api.model.type.component.Composite;
import org.fabric3.binding.kafka.generator.KafkaConnectionBindingGenerator;
import org.fabric3.binding.kafka.introspection.KafkaIntrospector;
import org.fabric3.binding.kafka.runtime.KafkaConnectionManagerImpl;
import org.fabric3.binding.kafka.runtime.KafkaConnectionSourceAttacher;
import org.fabric3.binding.kafka.runtime.KafkaConnectionTargetAttacher;
import org.fabric3.spi.model.type.system.SystemComponentBuilder;

/**
 *
 */
public class KafkaProvider {
    private static final QName QNAME = new QName(Namespaces.F3, "KafkaExtension");

    @Provides
    public static Composite getComposite() {
        CompositeBuilder compositeBuilder = CompositeBuilder.newBuilder(QNAME);
        compositeBuilder.component(SystemComponentBuilder.newBuilder(KafkaIntrospector.class).build());
        compositeBuilder.component(SystemComponentBuilder.newBuilder(KafkaConnectionBindingGenerator.class).build());
        compositeBuilder.component(SystemComponentBuilder.newBuilder(KafkaConnectionSourceAttacher.class).build());
        compositeBuilder.component(SystemComponentBuilder.newBuilder(KafkaConnectionTargetAttacher.class).build());

        SystemComponentBuilder managerBuilder = SystemComponentBuilder.newBuilder(KafkaConnectionManagerImpl.class);
        managerBuilder.reference("executorService", "RuntimeThreadPoolExecutor") ;
        compositeBuilder.component(managerBuilder.build());

        compositeBuilder.deployable();
        return compositeBuilder.build();
    }
}
