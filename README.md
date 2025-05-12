# tired-wire

Stream structured binary data into MongoDB to increase throughput.

This is an application-level add-on, which starts a HTTP server alongside the MongoDB server.
The HTTP server has two endpoints:

### `POST /schema`

Request body - AVRO schema that represents the data which is about to flow into server.
Response - the ID of the schema.

* `POST /stream/:id/:collection`

Request body - a stream of binary data serialized with the AVRO schema sent previously.
`:id` URI param - the ID of the schema sent previously.
`:collection` URI param - the name of the collection to write data to.

## Requirements

* dotnet 9.0+

## Building

```bash
$ dotnet build
```

## Running

```bash
$ dotnet run -- --mongo-url mongodb://host:port/database
```

This will start a server targeting the specified MongoDB database.

## Testing

There are two test commands in the `test-copy` application provided:

### Mongo-to-Mongo using native driver 

Uses MongoDB driver to copy batches of records from one database to another.

Usage:

```bash
$ dotnet run -- to-mongo --source-db mongodb://host:port/database --target-db mongodb://host:port/database --collection collection
```

### Mongo-to-Mongo using TiredWire

Uses Avro serialization and writes serialized records from one MongoDB database through the TiredWire auxilary server to another MongoDB database

Usage:

```bash
$ dotnet run -- to-tired-wire --source-db mongodb://host:port/database --destination http://host:port/ --collection collection --schema schema.avsc
```

## Benchmarking

Sample collection: `CyclicCrossMapping.orderDetails` containing `8923` documents.

### Debug

#### TiredWire

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 6.7799 ms; schema parse time: 30.7901 ms; schema write time: 129.1027 ms; serialization time: 0.003931558892748719 ms on average (0.0028 min, 2.2787 max, 35.081299999996816 ms total); writing to Avro stream time: 44.4593 ms (total serialization time: 79.54059999999681 ms); writing to TiredWire time: 145.8605 ms (total writing time: 225.4010999999968 ms); total-total time: 392.0737999999968 ms

+

Inserted 8923 documents; reading stream time: 0.00802820800179314 ms average (0.002 ms min, 8.0029 ms max, 71.6357000000002 ms total); deserialization time: 0.008054768575591132 ms (0.0018 ms min, 12.9545 ms max, 71.87269999999968 ms total); writing time: 55.20551555555554 ms avg (28.841 ms min, 636.0783 ms max, 4968.496399999999 ms total); total time: 5112.004799999999 ms

empty DB:

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 5.2835 ms; schema parse time: 55.1366 ms; schema write time: 268.8948 ms; serialization time: 0.0045231536478761835 ms on average (0.0027 min, 4.7111 max, 40.360099999999186 ms total); writing to Avro stream time: 44.9258 ms (total serialization time: 85.28589999999919 ms); writing to TiredWire time: 8.1474 ms (total writing time: 93.43329999999919 ms); total-total time: 422.7481999999992 ms

+

Inserted 8923 documents; reading stream time: 0.009940860697075088 ms average (0.002 ms min, 3.6746 ms max, 88.702300000001 ms total); deserialization time: 0.011277810153535782 ms (0.0017 ms min, 4.3508 ms max, 100.63189999999979 ms total); writing time: 60.88464555555555 ms avg (37.0472 ms min, 420.9504 ms max, 5479.6181 ms total); total time: 5668.952300000001 ms

empty collection:

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 5.6543 ms; schema parse time: 23.9035 ms; schema write time: 43.1225 ms; serialization time: 0.0041634652022860714 ms on average (0.0029 min, 3.8879 max, 37.15059999999862 ms total); writing to Avro stream time: 42.504 ms (total serialization time: 79.65459999999862 ms); writing to TiredWire time: 4.9059 ms (total writing time: 84.56049999999863 ms); total-total time: 157.24079999999864 ms

+

Inserted 8923 documents; reading stream time: 0.009945433150285836 ms average (0.002 ms min, 7.8787 ms max, 88.74310000000051 ms total); deserialization time: 0.010212215622548539 ms (0.0018 ms min, 1.5106 ms max, 91.12360000000061 ms total); writing time: 54.444851111111134 ms avg (35.319 ms min, 251.7952 ms max, 4900.036600000002 ms total); total time: 5079.903300000004 ms

#### Native driver

Bytes copied: 3689528; writing time: 1093.7231 ms average (86.3001 ms min, 2101.1461 ms max, 2187.4462 ms total)

empty DB:

Done! Documents written: 8923 (3689528 bytes); writing time: 2050.2738 ms average (153.2741 ms min, 3947.2735 ms max, 4100.5476 ms total)
Done! Documents written: 8923 (3689528 bytes); writing time: 2125.688 ms average (131.4317 ms min, 4119.9443 ms max, 4251.376 ms total)

empty collection:

Done! Documents written: 8923 (3689528 bytes); writing time: 2505.42295 ms average (337.2987 ms min, 4673.5472 ms max, 5010.8459 ms total)

### Release

#### TiredWire

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 5.4022 ms; schema parse time: 24.8674 ms; schema write time: 114.5924 ms; serialization time: 0.004974100638798548 ms on average (0.0035 min, 4.2449 max, 44.38389999999944 ms total); writing to Avro stream time: 44.1113 ms (total serialization time: 88.49519999999944 ms); writing to TiredWire time: 133.8925 ms (total writing time: 222.38769999999946 ms); total-total time: 367.24969999999945 ms

+

Inserted 8923 documents; reading stream time: 0.009049534909783696 ms average (0.0017 ms min, 18.5999 ms max, 80.74899999999992 ms total); deserialization time: 0.006459352235795158 ms (0.0011 ms min, 6.1311 ms max, 57.6368000000002 ms total); writing time: 60.898954444444435 ms avg (30.1032 ms min, 635.7611 ms max, 5480.905899999999 ms total); total time: 5619.291699999999 ms

empty DB:

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 5.7807 ms; schema parse time: 26.249 ms; schema write time: 46.3817 ms; serialization time: 0.005198856886697173 ms on average (0.0037 min, 4.3642 max, 46.38939999999888 ms total); writing to Avro stream time: 44.3972 ms (total serialization time: 90.78659999999888 ms); writing to TiredWire time: 7.3492 ms (total writing time: 98.13579999999888 ms); total-total time: 176.54719999999887 ms

+

Inserted 8923 documents; reading stream time: 0.011128555418581166 ms average (0.0017 ms min, 17.36 ms max, 99.30009999999974 ms total); deserialization time: 0.009426426089880067 ms (0.0011 ms min, 15.4168 ms max, 84.11199999999984 ms total); writing time: 73.53158222222218 ms avg (36.1628 ms min, 362.3104 ms max, 6617.842399999997 ms total); total time: 6801.254499999996 ms

empty collection:

Done! Documents serialized and posted: 8923 (1384906 bytes); schema read time: 5.3919 ms; schema parse time: 24.9197 ms; schema write time: 44.2059 ms; serialization time: 0.005142911576824155 ms on average (0.0037 min, 1.4423 max, 45.89020000000194 ms total); writing to Avro stream time: 45.3261 ms (total serialization time: 91.21630000000194 ms); writing to TiredWire time: 5.7723 ms (total writing time: 96.98860000000194 ms); total-total time: 171.50610000000194 ms

+

Inserted 8923 documents; reading stream time: 0.010051686652470984 ms average (0.0017 ms min, 1.117 ms max, 89.69119999999859 ms total); deserialization time: 0.00966256864283327 ms (0.0011 ms min, 4.4957 ms max, 86.21910000000128 ms total); writing time: 45.93816777777779 ms avg (32.7066 ms min, 115.5605 ms max, 4134.4351000000015 ms total); total time: 4310.345400000001 ms

#### Native driver

Done! Documents written: 8923 (3689528 bytes); writing time: 1340.6813 ms average (93.0456 ms min, 2588.317 ms max, 2681.3626 ms total)

empty DB:

Done! Documents written: 8923 (3689528 bytes); writing time: 1637.4623 ms average (117.6474 ms min, 3157.2772 ms max, 3274.9246 ms total)


empty collection:

Done! Documents written: 8923 (3689528 bytes); writing time: 1859.89045 ms average (122.2247 ms min, 3597.5562 ms max, 3719.7809 ms total)

