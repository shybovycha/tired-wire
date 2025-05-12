# tired-wire

Stream structured binary data into MongoDB to increase throughput.

This is an application-level add-on, which starts a HTTP server alongside the MongoDB server.
The HTTP server has two endpoints:

### `POST /`

Request body - AVRO schema that represents the data which is about to flow into server.
Response - the ID of the schema.

* `POST /:id/:collection`

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
$ dotnet run
```
