package io.confluent.connect.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleAvroPartitioner {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final Map<org.apache.avro.Schema, DatumWriter<Object>> datumWriterCache = new ConcurrentHashMap<>();
    private final AvroData avroData = new AvroData(2048);

    public Integer getPartition(
            org.apache.kafka.connect.data.Schema kafkaSchema,
            Object kafkaConnectObject,
            Integer numberOfPartitions
    ) {
        byte[] bytes = fromConnectData(kafkaSchema, kafkaConnectObject);
        return Utils.toPositive(Utils.murmur2(bytes)) % numberOfPartitions;
    }

    private byte[] fromConnectData(org.apache.kafka.connect.data.Schema kafkaSchema, Object kafkaConnectObject) {
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(kafkaSchema);
        Object avroObject = avroData.fromConnectData(kafkaSchema, avroSchema, kafkaConnectObject);
        return avroSerialize(avroObject, new AvroSchema(avroSchema));
    }

    private byte[] avroSerialize(
            Object avroObject,
            io.confluent.kafka.schemaregistry.avro.AvroSchema schema
    ) throws SerializationException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Object value = avroObject instanceof NonRecordContainer
                ? ((NonRecordContainer) avroObject).getValue()
                : avroObject;
        org.apache.avro.Schema rawSchema = schema.rawSchema();
        try {
            if (rawSchema.getType().equals(org.apache.avro.Schema.Type.BYTES)) {
                if (value instanceof byte[]) {
                    out.write((byte[]) value);
                } else if (value instanceof ByteBuffer) {
                    out.write(((ByteBuffer) value).array());
                } else {
                    throw new SerializationException(
                            "Unrecognized bytes object of type: " + value.getClass().getName());
                }
            } else {
                writeDatum(out, value, rawSchema);
            }
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | RuntimeException e) {
            // avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            throw new SerializationException("Error simple serializing Avro message", e);
        }
    }

    private void writeDatum(ByteArrayOutputStream out, Object value, org.apache.avro.Schema rawSchema)
            throws IOException {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

        DatumWriter<Object> writer;
        writer = datumWriterCache.computeIfAbsent(rawSchema,
                v -> (DatumWriter<Object>) getDatumWriter(rawSchema)
        );
        writer.write(value, encoder);
        encoder.flush();
    }

    private DatumWriter<?> getDatumWriter(org.apache.avro.Schema schema) {
        GenericData genericData = new GenericData();
        return new GenericDatumWriter<>(schema, genericData);
    }
}
