/*
 * Fabric3
 * Copyright (c) 2009-2015 Metaform Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fabric3.api.binding.kafka.model;

import java.net.URI;

import org.fabric3.api.model.type.builder.AbstractBuilder;

/**
 * Builder for the Kafka binding.
 */
public class KafkaBindingBuilder extends AbstractBuilder {
    private KafkaBinding binding;

    public static KafkaBindingBuilder newBuilder() {
        return new KafkaBindingBuilder();
    }

    public KafkaBindingBuilder(String name) {
        this.binding = new KafkaBinding();
    }

    public KafkaBindingBuilder uri(URI uri) {
        checkState();
        binding.setTarget(uri);
        return this;
    }

    public KafkaBindingBuilder defaultTopic(String topic) {
        checkState();
        binding.setDefaultTopic(topic);
        return this;
    }

    public KafkaBindingBuilder configuration(String key, String value) {
        checkState();
        binding.addConfig(key, value);
        return this;
    }

    public KafkaBinding build() {
        checkState();
        freeze();
        return binding;
    }

    private KafkaBindingBuilder() {
        this("kafka.binding");
    }

}
