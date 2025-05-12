using System.Collections.Concurrent;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using System.CommandLine;  
using System.CommandLine.NamingConventionBinder;
using System.Diagnostics;

var rootCommand = new RootCommand("Stream data into MongoDB")
{
    new Option<string>(
        name: "--mongo-url",
        description: "MongoDB connection string (e.g., mongodb://localhost:27017/mydb)",
        getDefaultValue: () => ""
    )
    {
        IsRequired = true,
    },
};

rootCommand.Handler = CommandHandler.Create<string>(
    (mongoUrl) =>
    {
        RunApp(mongoUrl);
    }
);

return await rootCommand.InvokeAsync(args);

void RunApp(string mongoUrl)
{
    if (string.IsNullOrEmpty(mongoUrl))
    {
        Console.Error.WriteLine($"--mongo-url is blank");
        return;
    }

    // For top-level statements compatibility
    var builder = WebApplication.CreateSlimBuilder();

    builder.Services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoUrl));

    var schemaDict = new ConcurrentDictionary<string, Avro.Schema>();

    var app = builder.Build();

    app.MapPost(
        "/schema",
        async (HttpRequest request, ILogger<Program> logger) =>
        {
            using var reader = new StreamReader(request.Body, Encoding.UTF8);
            var schemaJson = await reader.ReadToEndAsync();

            try
            {
                var schema = Avro.Schema.Parse(schemaJson);

                var id = Guid.NewGuid().ToString();
                schemaDict[id] = schema;

                return Results.Text(id, "text/plain", statusCode: StatusCodes.Status201Created);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Can not parse schema");
                return Results.BadRequest(new { error = $"Invalid Avro schema: {ex.Message}" });
            }
        }
    );

    app.MapPost(
        "/stream/{id}/{collection}",
        async (string id, string collection, IMongoClient mongoClient, HttpRequest request) =>
        {
            if (!schemaDict.TryGetValue(id, out var avroSchema))
            {
                return Results.NotFound(new { error = "Schema not found" });
            }

            // 2. Read the whole request body as a MemoryStream  
            var mem = new MemoryStream();  

            try
            {
                await request.Body.CopyToAsync(mem);  
                mem.Position = 0; // rewind
            }
            catch
            {
                return Results.BadRequest("Unable to read request body");
            }

            _ = Task.Run(async () =>
            {
                var mongoDbName = MongoUrl.Create(mongoUrl).DatabaseName ?? "test";
                var db = mongoClient.GetDatabase(mongoDbName);
                var coll = db.GetCollection<BsonDocument>(collection);

                try
                {
                    var decoder = new BinaryDecoder(mem);
                    var reader = new GenericReader<GenericRecord>(avroSchema, avroSchema);

                    var buffer = new List<BsonDocument>();
                    const int batchSize = 100;
                    int count = 0;
                    var readingDurations = new List<double>();
                    var deserializationDurations = new List<double>();
                    var writeToMongoDurations = new List<double>();

                    while (true)
                    {
                        var _sw0 = Stopwatch.StartNew();
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
                        readingDurations.Add(_sw0.Elapsed.TotalMilliseconds);

                        var _sw1 = Stopwatch.StartNew();
                        var doc = new BsonDocument();
                        foreach (var field in ((Avro.RecordSchema)avroSchema).Fields)
                        {
                            var val = record[field.Name];
                            doc[field.Name] = BsonValueCreate(val);
                        }
                        buffer.Add(doc);
                        count++;
                        deserializationDurations.Add(_sw1.Elapsed.TotalMilliseconds);

                        var _sw2 = Stopwatch.StartNew();
                        if (buffer.Count >= batchSize)
                        {
                            await coll.InsertManyAsync(buffer);
                            buffer.Clear();
                            writeToMongoDurations.Add(_sw2.Elapsed.TotalMilliseconds);
                        }
                    }

                    if (buffer.Count > 0)
                    {
                        var _sw3 = Stopwatch.StartNew();
                        await coll.InsertManyAsync(buffer);
                        writeToMongoDurations.Add(_sw3.Elapsed.TotalMilliseconds);
                    }

                    app.Logger.LogInformation("Inserted {count} documents; reading stream time: {readingAvg} ms average ({readingMin} ms min, {readingMax} ms max, {readingSum} ms total); deserialization time: {deserializationAvg} ms ({deserializationMin} ms min, {deserializationMax} ms max, {deserializationTotal} ms total); writing time: {writingAvg} ms avg ({writingMin} ms min, {writingMax} ms max, {writingTotal} ms total); total time: {totalTime} ms", count, readingDurations.Average(), readingDurations.Min(), readingDurations.Max(), readingDurations.Sum(), deserializationDurations.Average(), deserializationDurations.Min(), deserializationDurations.Max(), deserializationDurations.Sum(), writeToMongoDurations.Average(), writeToMongoDurations.Min(), writeToMongoDurations.Max(), writeToMongoDurations.Sum(), readingDurations.Sum() + deserializationDurations.Sum() + writeToMongoDurations.Sum());

                    return Results.Ok(new { inserted = count });
                }
                catch (Exception ex)
                {
                    return Results.BadRequest(new { error = ex.Message });
                }
                finally
                {
                    mem.Dispose();
                }
            });

            return Results.Accepted();
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
}
