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
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.kafka.common.utils.Utils;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class InsertUuid<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Insert a random UUID into a connect record";

  private interface ConfigName {
    String UUID_FIELD_NAME = "uuid.field.name";
    String UUID_PARTITION_NUMBER = "uuid.partition.number";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.UUID_FIELD_NAME, ConfigDef.Type.STRING, "uuid", ConfigDef.Importance.HIGH,
      "Field name for UUID")
    .define(ConfigName.UUID_PARTITION_NUMBER, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH,
      "Number of partitions in a topic");

  private static final String PURPOSE = "adding UUID to record";

  private String fieldName;
  private Integer numberOfPartitions;

  private Cache<Schema, Schema> schemaUpdateCache;

  public final MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
  private final AvroConverter avroConverter = new AvroConverter(mockSchemaRegistryClient);

  private final SimpleAvroPartitioner serializer = new SimpleAvroPartitioner();


  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(ConfigName.UUID_FIELD_NAME);
    numberOfPartitions = config.getInt(ConfigName.UUID_PARTITION_NUMBER);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
//    "schema.registry.url" -> "https://apicurio-dev.bi-wind.co/apis/ccompat/v6"
//    MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();

    avroConverter.configure(Collections.singletonMap("schema.registry.url", ""), true);
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
//    final Integer partition = calculatePartition(value);
    // final Integer partition = Utils.toPositive(Utils.murmur2(value.toString().getBytes())) % numberOfPartitions;

//    final Integer partition = Utils.toPositive(Utils.murmur2(
//      avroConverter.fromConnectData("", value.schema(), operatingValue(record))
//    )) % numberOfPartitions;

//    serializer.serialize(object, value.schema());
    final Integer partition = serializer.getPartition(value.schema(), object, numberOfPartitions);

    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if(updatedSchema == null) {
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


  private Integer calculatePartition(Struct value) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      ObjectOutputStream out;
      out = new ObjectOutputStream(bos);
//      for (Field field : value.schema().fields()) {
//        if (field.schema().type().isPrimitive()) {
//          out.writeObject(value.get(field));
//        }
//      }
      serialize(value, out);
      out.flush();
      byte[] bytes = bos.toByteArray();
      return Utils.toPositive(Utils.murmur2(bytes)) % numberOfPartitions;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private void serialize(Struct struct, ObjectOutputStream out) throws IOException {
    for (Field field : struct.schema().fields()) {
      Schema.Type type = field.schema().type();
      Object value = struct.get(field);
      if (type.isPrimitive()) {
        out.writeObject(value);
      } else {
        switch (type) {
          case ARRAY:
            List<?> array = (List)value;
            for (Object entry : array) {
              if (entry instanceof Struct) {
                serialize((Struct) entry, out);
              } else {
                out.writeObject(value);
              }
            }
            break;
          case MAP:
            Map<?, ?> map = (Map)value;
            Iterator it = map.entrySet().iterator();
            while(it.hasNext()) {
              Map.Entry<?, ?> entry = (Map.Entry)it.next();
              if (entry.getKey() instanceof Struct) {
                serialize((Struct) entry.getKey(), out);
              } else {
                out.writeObject(entry.getKey());
              }
              if (entry.getValue() instanceof Struct) {
                serialize((Struct) entry.getValue(), out);
              } else {
                out.writeObject(entry.getValue());
              }
            }
            break;
          case STRUCT:
            serialize((Struct) value, out);
            break;
        }
      }
    }
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

    for (Field field: schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Integer partition);

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
//      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
      return record.newRecord(record.topic(), partition, updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
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
      return record.newRecord(record.topic(), partition, record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


