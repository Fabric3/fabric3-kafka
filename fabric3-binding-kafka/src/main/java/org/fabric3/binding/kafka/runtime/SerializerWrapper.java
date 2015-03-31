package org.fabric3.binding.kafka.runtime;

import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serializer;

/**
 *
 */
public class SerializerWrapper implements Serializer {
    private Function<Object, byte[]> function;

    public SerializerWrapper(Function<Object, byte[]> function) {
        this.function = function;
    }

    public byte[] serialize(String topic, Object data) {
        return function.apply(data);
    }

    public void configure(Map configs, boolean isKey) {

    }

    public void close() {

    }
}
