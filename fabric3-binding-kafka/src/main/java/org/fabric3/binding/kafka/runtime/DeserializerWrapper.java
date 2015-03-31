package org.fabric3.binding.kafka.runtime;

import java.util.function.Function;

import kafka.serializer.Decoder;

/**
 *
 */
public class DeserializerWrapper implements Decoder {
    private Function<byte[], Object> function;

    public DeserializerWrapper(Function<byte[], Object> function) {
        this.function = function;
    }

    public Object fromBytes(byte[] bytes) {
        return function.apply(bytes);
    }
}
