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

Mongo-to-Mongo using native driver: `3689528` bytes in `?` ms.
Mongo-to-Mongo using TiredWire: `1384906` bytes in `?` ms.
Sample collection: `CyclicCrossMapping.orderDetails` containing `8923` documents.

