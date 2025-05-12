using System.Collections.Concurrent;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

// For top-level statements compatibility
var builder = WebApplication.CreateSlimBuilder(args);

// Parse the MongoDB URL passed via --mongo-url
string mongoUrl = null;
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--mongo-url" && args.Length > i + 1)
    {
        mongoUrl = args[i + 1];
        break;
    }
}

if (string.IsNullOrWhiteSpace(mongoUrl))
{
    Console.WriteLine("Usage: dotnet run -- --mongo-url <MongoDB connection string>");
    return;
}

builder.Services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoUrl));

var schemaDict = new ConcurrentDictionary<string, string>();

var app = builder.Build();

app.MapPost(
    "/schema",
    async (HttpRequest request) =>
    {
        using var reader = new StreamReader(request.Body, Encoding.UTF8);
        var schemaJson = await reader.ReadToEndAsync();

        try
        {
            var schema = Avro.Schema.Parse(schemaJson);

            var id = Guid.NewGuid().ToString();
            schemaDict[id] = schemaJson;

            return Results.Ok(new { id });
        }
        catch (Exception ex)
        {
            return Results.BadRequest(new { error = $"Invalid Avro schema: {ex.Message}" });
        }
    }
);

app.MapPost(
    "/stream/{id}/{collection}",
    async (string id, string collection, IMongoClient mongoClient, HttpRequest request) =>
    {
        if (!schemaDict.TryGetValue(id, out var schemaJson))
        {
            return Results.NotFound(new { error = "Schema not found" });
        }

        Avro.Schema avroSchema;
        try
        {
            avroSchema = Avro.Schema.Parse(schemaJson);
        }
        catch (Exception ex)
        {
            return Results.BadRequest(new { error = $"Stored schema invalid: {ex.Message}" });
        }

        var mongoDbName = MongoUrl.Create(mongoUrl).DatabaseName ?? "test";
        var db = mongoClient.GetDatabase(mongoDbName);
        var coll = db.GetCollection<BsonDocument>(collection);

        try
        {
            var decoder = new BinaryDecoder(request.Body);
            var reader = new GenericReader<GenericRecord>(avroSchema, avroSchema);

            var buffer = new List<BsonDocument>();
            const int batchSize = 100;
            int count = 0;

            while (true)
            {
                GenericRecord record;
                try
                {
                    record = reader.Read(null, decoder);
                }
                catch (Avro.AvroException)
                {
                    // EOF (or stream ended), exit loop
                    break;
                }

                var doc = new BsonDocument();
                foreach (var field in ((Avro.RecordSchema)avroSchema).Fields)
                {
                    var val = record[field.Name];
                    doc[field.Name] = BsonValueCreate(val);
                }
                buffer.Add(doc);
                count++;

                if (buffer.Count >= batchSize)
                {
                    await coll.InsertManyAsync(buffer);
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0)
            {
                await coll.InsertManyAsync(buffer);
            }

            return Results.Ok(new { inserted = count });
        }
        catch (Exception ex)
        {
            return Results.BadRequest(new { error = ex.Message });
        }
    }
);

static BsonValue BsonValueCreate(object val)
{
    if (val == null)
        return BsonNull.Value;

    return val switch
    {
        int i => new BsonInt32(i),
        long l => new BsonInt64(l),
        float f => new BsonDouble(f),
        double d => new BsonDouble(d),
        bool b => new BsonBoolean(b),
        string s => new BsonString(s),
        IDictionary<string, object> dict => new BsonDocument(
            dict.Select(kv => new BsonElement(kv.Key, BsonValueCreate(kv.Value)))
        ),
        GenericRecord record => new BsonDocument(
            record.Schema.Fields.Select(f => new BsonElement(
                f.Name,
                BsonValueCreate(record[f.Name])
            ))
        ),
        IEnumerable<object> arr => new BsonArray(arr.Select(BsonValueCreate)),
        _ => BsonValue.Create(val),
    };
}

app.Run();
