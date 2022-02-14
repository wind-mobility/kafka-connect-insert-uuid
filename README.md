Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

This SMT supports inserting a UUID into the record Key or Value Properties:

| Name                                          | Description                                                                                                             | Type    | Default | Importance |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|---------|---------|------------|
| `uuid.field.name`                             | Field name for UUID                                                                                                     | String  | `uuid`  | High       |
| `uuid.calculate.partition.before.adding.uuid` | Shall calculate the partition before inserting the UUID (only for Key). It uses the Avro serializer for this purpose    | Boolean | `true`  | High       |
| `uuid.number.of.partitions`                   | Number of partitions in a topic                                                                                         | Integer | `0`     | High       |
| `uuid.schema.name.when.null`                  | Schema name when operating value is null (usually 'Key' or 'Envelope' when target struct is key or value, respectively) | String  | `uuid`  | High       |

Example on how to add to your connector:

```
transforms=insertuuid
transforms.insertuuid.type=co.data.wind.kafka.connect.smt.InsertUuid$Key
transforms.insertuuid.uuid.field.name="uuid"
```

UUID logic borrowed from [cjmatta original project](https://github.com/cjmatta/kafka-connect-insert-uuid)
