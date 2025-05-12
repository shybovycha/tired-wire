using System.Diagnostics;
using System.CommandLine;
using System.Linq;
using System.Net.Http;
using System.Text;
using Avro;
using Avro.Generic;
using Avro.IO;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// CLI options
var sourceConnectionOption = new Option<string>(
    "--source-db",
    description: "MongoDB connection string for source (e.g. mongodb://localhost:27017/source_db)"
);

var targetConnectionOption = new Option<string>(
    "--target-db",
    description: "MongoDB connection string for target"
)
{
    IsRequired = true,
};

var collectionOption = new Option<string>(
    "--collection",
    description: "Name of the collection to copy"
)
{
    IsRequired = true,
};

var schemaOption = new Option<string>(
    "--schema",
    description: "Path to Avro schema file (e.g. ./schema.avsc)"
)
{
    IsRequired = true,
};

var destinationOption = new Option<string>(
    "--target-tiredwire",
    description: "Target TiredWire server base URL (e.g. https://receiver.example/)"
)
{
    IsRequired = true,
};

var rootCommand = new RootCommand();

var copyToMongo = new Command("to-mongo", "Stream MongoDB collection to MongoDB using driver");
copyToMongo.Add(sourceConnectionOption);
copyToMongo.Add(targetConnectionOption);
copyToMongo.Add(collectionOption);

var copyToTiredWire = new Command(
    "to-tired-wire",
    "Stream MongoDB collection to TiredWire auxilary server"
);
copyToTiredWire.Add(sourceConnectionOption);
copyToTiredWire.Add(collectionOption);
copyToTiredWire.Add(destinationOption);
copyToTiredWire.Add(schemaOption);

rootCommand.Add(copyToMongo);
rootCommand.Add(copyToTiredWire);

GenericRecord ConvertBsonToAvro(BsonDocument bsonDoc, Avro.Schema recordSchema)
{
    object? MapBsonToAvro(BsonValue val, Schema schema)
    {
        switch (schema.Tag)
        {
            case Schema.Type.Boolean:
                return val.IsBsonNull ? null : val.AsBoolean;
            case Schema.Type.Int:
                return val.IsBsonNull ? null
                    : val.IsInt32 ? val.AsInt32
                    : Convert.ToInt32(val);
            case Schema.Type.Long:
                return val.IsBsonNull ? null
                    : val.IsInt64 ? val.AsInt64
                    : Convert.ToInt64(val);
            case Schema.Type.Float:
                return val.IsBsonNull ? null
                    : val.IsDouble ? (float)val.AsDouble
                    : Convert.ToSingle(val);
            case Schema.Type.Double:
                return val.IsBsonNull ? null
                    : val.IsDouble ? val.AsDouble
                    : Convert.ToDouble(val);
            case Schema.Type.String:
                return val.IsBsonNull ? null : val.AsString;
            case Schema.Type.Bytes:
                return val.IsBsonNull ? null : val.AsByteArray;
            case Schema.Type.Null:
                return null;
            case Schema.Type.Enumeration:
                return val.IsBsonNull ? null
                    : ((EnumSchema)schema).Symbols.Contains(val.AsString) ? val.AsString
                    : throw new Exception($"Value '{val}' not in Avro enum.");
            case Schema.Type.Record:
                if (val.IsBsonNull)
                    return null;
                var recSchema = (RecordSchema)schema;

                // SPECIAL CASE: BSON DateTime to Avro { "$date": "string" }
                if (
                    val.BsonType == BsonType.DateTime
                    && recSchema.Fields.Count == 1
                    && recSchema.Fields[0].Name == "$date"
                    && recSchema.Fields[0].Schema.Tag == Schema.Type.String
                )
                {
                    var dateRecord = new GenericRecord(recSchema);
                    dateRecord.Add("$date", val.ToUniversalTime().ToString("o"));
                    return dateRecord;
                }

                var recDoc = val.AsBsonDocument;
                var rec = new GenericRecord(recSchema);
                foreach (var field in recSchema.Fields)
                    rec.Add(
                        field.Name,
                        recDoc.TryGetValue(field.Name, out var subVal)
                            ? MapBsonToAvro(subVal, field.Schema)
                            : MapBsonToAvro(BsonNull.Value, field.Schema)
                    );
                return rec;
            case Schema.Type.Array:
                if (val.IsBsonNull)
                    return null;
                var arraySchema = (ArraySchema)schema;
                var list = new List<object>();
                foreach (var item in val.AsBsonArray)
                    list.Add(MapBsonToAvro(item, arraySchema.ItemSchema));
                return list;
            case Schema.Type.Map:
                if (val.IsBsonNull)
                    return null;
                var mapSchema = (MapSchema)schema;
                var mapObj = val.AsBsonDocument;
                var dict = new Dictionary<string, object>();
                foreach (var prop in mapObj)
                    dict[prop.Name] = MapBsonToAvro(prop.Value, mapSchema.ValueSchema);
                return dict;
            case Schema.Type.Union:
                var unionSchema = (UnionSchema)schema;
                if (val.IsBsonNull)
                {
                    foreach (var branch in unionSchema.Schemas)
                        if (branch.Tag == Schema.Type.Null)
                            return null;
                    throw new Exception("Null found but Avro union does not support null.");
                }
                foreach (var branch in unionSchema.Schemas)
                {
                    try
                    {
                        return MapBsonToAvro(val, branch);
                    }
                    catch { }
                }
                throw new Exception($"No union branch matched for value: {val}");
            case Schema.Type.Fixed:
                return val.IsBsonNull ? null : val.AsByteArray;
            default:
                throw new NotSupportedException($"Unsupported Avro schema type: {schema.Tag}");
        }
    }

    return (GenericRecord)MapBsonToAvro(bsonDoc, recordSchema);
}

copyToMongo.SetHandler(
    async (sourceDbUrl, targetDbUrl, coll) =>
    {
        var sourceClient = new MongoClient(sourceDbUrl);
        var sourceDatabase = sourceClient.GetDatabase(
            new MongoUrlBuilder(sourceDbUrl).DatabaseName
        );

        var targetClient = new MongoClient(targetDbUrl);
        var targetDatabase = targetClient.GetDatabase(
            new MongoUrlBuilder(targetDbUrl).DatabaseName
        );

        var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(coll);
        var targetCollection = targetDatabase.GetCollection<BsonDocument>(coll);

        var records = new List<GenericRecord>();
        var durations = new List<double>();
        using (var cursor = await sourceCollection.FindAsync(FilterDefinition<BsonDocument>.Empty))
        {
            int copied = 0;
            int bytesCopied = 0;

            while (await cursor.MoveNextAsync())
            {
                var batch = cursor.Current.ToList();

                var size = batch.Sum(doc => doc.ToBson().Length);

                if (batch.Count > 0)
                {
                    var sw = Stopwatch.StartNew();
                    batch.ForEach(doc => doc.Remove("_id"));
                    await targetCollection.InsertManyAsync(batch);
                    durations.Add(sw.Elapsed.TotalMilliseconds);
                    copied += batch.Count;
                    bytesCopied += size;
                }
            }

            Console.WriteLine($"Done! Documents written: {copied} ({bytesCopied} bytes); writing time: {durations.Average()} ms average ({durations.Min()} ms min, {durations.Max()} ms max, {durations.Sum()} ms total)");
        }
    },
    sourceConnectionOption,
    targetConnectionOption,
    collectionOption
);

copyToTiredWire.SetHandler(
    async (srcConn, coll, destination, schemaPath) =>
    {
        // 1. Read and parse AVRO schema
        var schemaReadTime = 0.0;
        string avroSchemaJson;
        try
        {
            var _sw0 = Stopwatch.StartNew();
            avroSchemaJson = await File.ReadAllTextAsync(schemaPath);
            schemaReadTime = _sw0.Elapsed.TotalMilliseconds;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading Avro schema file '{schemaPath}': {ex.Message}");
            return;
        }

        // 2. POST schema to destination/schema and store response in id
        var schemaWriteTime = 0.0;
        var _sw1 = Stopwatch.StartNew();
        string schemaUrl = $"{destination.TrimEnd('/')}/schema";
        string id;
        using (var httpClient = new HttpClient())
        {
            var postContent = new StringContent(avroSchemaJson, Encoding.UTF8, "application/json");
            HttpResponseMessage response;
            try
            {
                Console.WriteLine($"Posting schema to {schemaUrl} ...");
                response = await httpClient.PostAsync(schemaUrl, postContent);
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine(
                        $"Schema POST failed: {response.StatusCode} {response.ReasonPhrase}"
                    );
                    return;
                }
                id = await response.Content.ReadAsStringAsync();
                schemaWriteTime = _sw1.Elapsed.TotalMilliseconds;
                Console.WriteLine($"Schema POST succeeded. id: {id}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"HTTP error posting schema: {ex.Message}");
                return;
            }
        }

        // 3. Parse Avro schema
        var schemaParseTime = 0.0;
        Schema avroSchema;
        try
        {
            var _sw2 = Stopwatch.StartNew();
            avroSchema = Schema.Parse(avroSchemaJson);
            if (avroSchema.Tag != Schema.Type.Record)
            {
                Console.WriteLine("Only Avro Record schemas are supported.");
                return;
            }
            schemaParseTime = _sw2.Elapsed.TotalMilliseconds;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing Avro schema: {ex.Message}");
            return;
        }

        // 4. Set up MongoDB source
        string sourceDb = new MongoUrlBuilder(srcConn).DatabaseName;
        if (string.IsNullOrWhiteSpace(sourceDb))
        {
            Console.WriteLine("Error: Database name must be specified in the connection URL.");
            return;
        }
        var sourceClient = new MongoClient(srcConn);
        var sourceDatabase = sourceClient.GetDatabase(sourceDb);
        var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(coll);

        // 5. Fetch and convert documents to GenericRecord (Avro)
        var records = new List<GenericRecord>();
        var serializationDurations = new List<double>();
        using (var cursor = await sourceCollection.FindAsync(FilterDefinition<BsonDocument>.Empty))
        {
            while (await cursor.MoveNextAsync())
            {
                foreach (var doc in cursor.Current)
                {
                    var sw = Stopwatch.StartNew();
                    var record = ConvertBsonToAvro(doc, (RecordSchema)avroSchema);
                    serializationDurations.Add(sw.Elapsed.TotalMilliseconds);
                    records.Add(record);
                }
            }
        }

        var writingStreamTime = 0.0;

        // 6. Serialize records to Avro binary
        using var avroStream = new MemoryStream();

        {
            var sw = Stopwatch.StartNew();
            var writer = new BinaryEncoder(avroStream);
            var genericWriter = new GenericWriter<GenericRecord>(avroSchema);

            foreach (var rec in records)
            {
                genericWriter.Write(rec, writer);
            }

            writingStreamTime = sw.Elapsed.TotalMilliseconds;
        }

        var avroStreamLength = avroStream.Length;

        Console.WriteLine($"Bytes copied: {avroStream.Length}");
        avroStream.Seek(0, SeekOrigin.Begin);

        // 7. POST the Avro binary stream
        string streamUrl = $"{destination.TrimEnd('/')}/stream/{id}/{coll}";

        var writingToTiredWireTime = 0.0;

        var _sw = Stopwatch.StartNew();

        using (var httpClient = new HttpClient())
        using (var streamContent = new StreamContent(avroStream))
        {
            streamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
                "application/octet-stream"
            );
            try
            {
                Console.WriteLine($"Posting Avro stream to {streamUrl} ...");
                var streamResponse = await httpClient.PostAsync(streamUrl, streamContent);
                if (!streamResponse.IsSuccessStatusCode)
                {
                    Console.WriteLine(
                        $"Stream POST failed: {streamResponse.StatusCode} {streamResponse.ReasonPhrase}"
                    );
                    return;
                }
                Console.WriteLine("Success: Avro data posted to destination.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"HTTP error posting Avro stream: {ex.Message}");
                return;
            }
        }

        writingToTiredWireTime = _sw.Elapsed.TotalMilliseconds;

        Console.WriteLine($"Done! Documents serialized and posted: {records.Count} ({avroStreamLength} bytes; schema read time: {schemaReadTime} ms; schema parse time: {schemaParseTime} ms; schema write time: {schemaWriteTime} ms; serialization time: {serializationDurations.Average()} ms on average ({serializationDurations.Min()} min, {serializationDurations.Max()} max, {serializationDurations.Sum()} ms total); writing to Avro stream time: {writingStreamTime} ms (total serialization time: {writingStreamTime + serializationDurations.Sum()} ms); writing to TiredWire time: {writingToTiredWireTime} ms (total writing time: {writingStreamTime + serializationDurations.Sum() + writingToTiredWireTime} ms); total-total time: {writingStreamTime + serializationDurations.Sum() + writingToTiredWireTime + schemaReadTime + schemaParseTime + schemaWriteTime} ms");
    },
    sourceConnectionOption,
    collectionOption,
    destinationOption,
    schemaOption
);

return await rootCommand.InvokeAsync(args);
