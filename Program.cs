using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Avro.Generic;
using Avro.IO;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

public class Startup
{
    private readonly ConcurrentDictionary<string, string> schemaStore = new ConcurrentDictionary<string, string>();

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddRouting();
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseRouting();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapPost("/schema", async context =>
            {
                using var reader = new StreamReader(context.Request.Body);
                string schema = await reader.ReadToEndAsync();

                var id = Guid.NewGuid().ToString();
                schemaStore.TryAdd(id, schema);

                context.Response.StatusCode = 204;
                await context.Response.WriteAsync($"Schema stored with ID: {id}");
            });

            endpoints.MapPost("/stream/{id}/{collection}", async context =>
            {
                var routeValues = context.Request.RouteValues;
                var id = routeValues["id"] as string;
                var collection = routeValues["collection"] as string;

                if (!schemaStore.TryGetValue(id, out string schema))
                {
                  await context.Response.WriteAsync("Schema ID not found");
                  return;
                }

                var mongoClient = new MongoClient("mongodb://localhost:27017");
                var database = mongoClient.GetDatabase("avro_database");
                var collection = database.GetCollection<BsonDocument>(collection);

                using var reader = new BinaryReader(context.Request.Body);
                byte[] dataBytes = reader.ReadBytes((int)context.Request.Body.Length);

                var parsedSchema = Avro.Schema.Parse(schema) as Avro.RecordSchema;
                var record = new GenericDatumReader<GenericRecord>(parsedSchema, parsedSchema);

                using var stream = new MemoryStream(dataBytes);
                var decoder = new BinaryDecoder(stream);
                GenericRecord genericRecord = record.Read(null, decoder);

                var bsonRecord = new BsonDocument();

                foreach (var field in parsedSchema.Fields)
                {
                    bsonRecord.Add(field.Name, genericRecord[field.Name]?.ToString() ?? string.Empty);
                }

                await collection.InsertOneAsync(bsonRecord);
                await context.Response.WriteAsync("AVRO data processed and stored");
            });
        });
    }
}

public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseStartup<Startup>();
            });
}
