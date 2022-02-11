/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.cjmatta.kafka.connect.smt;

import io.confluent.connect.avro.SimpleAvroPartitioner;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertUuid<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Insert a random UUID into a connect record, optionally with partition calculation if is key";

    private interface ConfigName {
        String UUID_FIELD_NAME = "uuid.field.name";
        String UUID_CALCULATE_PARTITION_BEFORE_ADDING_UUID = "uuid.calculate.partition.before.adding.uuid";
        String UUID_NUMBER_OF_PARTITIONS = "uuid.number.of.partitions";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.UUID_FIELD_NAME, ConfigDef.Type.STRING, "uuid", ConfigDef.Importance.HIGH,
                    "Field name for UUID")
            .define(ConfigName.UUID_CALCULATE_PARTITION_BEFORE_ADDING_UUID,
                    ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Number of partitions in a topic")
            .define(ConfigName.UUID_NUMBER_OF_PARTITIONS, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH,
                    "Number of partitions in a topic");

    private static final String PURPOSE = "adding UUID to record";

    private String fieldName;
    private Integer numberOfPartitions;
    private Boolean calculatePartitionBeforeAddingUUID;

    private Cache<Schema, Schema> schemaUpdateCache;

    private final SimpleAvroPartitioner partitioner = new SimpleAvroPartitioner();

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.UUID_FIELD_NAME);
        numberOfPartitions = config.getInt(ConfigName.UUID_NUMBER_OF_PARTITIONS);
        calculatePartitionBeforeAddingUUID = config.getBoolean(
                ConfigName.UUID_CALCULATE_PARTITION_BEFORE_ADDING_UUID);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        updatedValue.put(fieldName, getRandomUuid());

        return newRecord(record, null, updatedValue, null);
    }

    private R applyWithSchema(R record) {
        final Object object = operatingValue(record);
        final Struct value = requireStruct(object, PURPOSE);

        Integer partition = record.kafkaPartition();
        if (calculatePartitionBeforeAddingUUID && isKey()) {
            partition = partitioner.getPartition(value.schema(), object, numberOfPartitions);
        }

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(fieldName, getRandomUuid());

        return newRecord(record, updatedSchema, updatedValue, partition);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private String getRandomUuid() {
        return UUID.randomUUID().toString();
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(fieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Integer partition);

    protected abstract boolean isKey();

    public static class Key<R extends ConnectRecord<R>> extends InsertUuid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Integer partition) {
            return record.newRecord(record.topic(), partition, updatedSchema,
                    updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

        @Override
        protected boolean isKey() {
            return true;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends InsertUuid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Integer partition) {
            return record.newRecord(record.topic(), partition, record.keySchema(),
                    record.key(), updatedSchema, updatedValue, record.timestamp());
        }

        @Override
        protected boolean isKey() {
            return false;
        }
    }
}


