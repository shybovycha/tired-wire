using MongoDB.Bson;  
using MongoDB.Driver;  
using System.CommandLine;  
using System.Net.Http;  
using System.Text;  
using Newtonsoft.Json;  
using Avro;  
using Avro.Generic;  
using Avro.IO;  
  
// CLI options  
var sourceConnectionOption = new Option<string>(  
    "--source-connection",  
    description: "MongoDB connection string for source (e.g. mongodb://localhost:27017/source_db)",  
    getDefaultValue: () => "mongodb://localhost:27017/source_db"  
);  
  
var collectionOption = new Option<string>(  
    "--collection",  
    description: "Name of the collection to copy")  
{ IsRequired = true };  
  
var schemaOption = new Option<string>(  
    "--schema",  
    description: "Path to AVRO schema file (e.g. ./schema.avsc)")  
{ IsRequired = true };  
  
var destinationOption = new Option<string>(  
    "--destination",  
    description: "HTTP URL destination (e.g. https://receiver.example/)")  
{ IsRequired = true };  
  
var rootCommand = new RootCommand("Stream MongoDB collection as Avro using given schema to an HTTP endpoint")  
{  
    sourceConnectionOption,  
    collectionOption,  
    schemaOption,  
    destinationOption  
};  
  
rootCommand.SetHandler(  
    async (srcConn, coll, schemaPath, destination) =>  
    {  
        // 1. Read and parse AVRO schema  
        string avroSchemaJson;  
        try  
        {  
            avroSchemaJson = await File.ReadAllTextAsync(schemaPath);  
        }  
        catch (Exception ex)  
        {  
            Console.WriteLine($"Error reading Avro schema file: {ex.Message}");  
            return;  
        }  
  
        // 2. POST schema to destination/schema and store response in id  
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
                    Console.WriteLine($"Schema POST failed: {response.StatusCode} {response.ReasonPhrase}");  
                    return;  
                }  
                id = await response.Content.ReadAsStringAsync();  
                Console.WriteLine($"Schema POST succeeded. id: {id}");  
            }  
            catch (Exception ex)  
            {  
                Console.WriteLine($"HTTP error posting schema: {ex.Message}");  
                return;  
            }  
        }  
  
        // 3. Parse Avro schema  
        Schema avroSchema;  
        try  
        {  
            avroSchema = Schema.Parse(avroSchemaJson);  
            if (avroSchema.Tag != Schema.Type.Record)  
            {  
                Console.WriteLine("Only Avro Record schemas are supported.");  
                return;  
            }  
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
        using (var cursor = await sourceCollection.FindAsync(FilterDefinition<BsonDocument>.Empty))  
        {  
            while (await cursor.MoveNextAsync())  
            {  
                foreach (var doc in cursor.Current)  
                {  
                    // Convert BsonDocument to a Dictionary<string, object>  
                    var json = doc.ToJson();  
                    var dict = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);  
  
                    // Create GenericRecord  
                    var record = new GenericRecord((RecordSchema)avroSchema);  
                    foreach (var field in ((RecordSchema)avroSchema).Fields)  
                    {  
                        // Assign value if present in MongoDB doc  
                        if (dict!.TryGetValue(field.Name, out var value))  
                        {  
                            // Convert value to the appropriate Avro type if necessary  
                            record.Add(field.Name, value is Newtonsoft.Json.Linq.JValue v ? v.Value : value);  
                        }  
                        else  
                        {  
                            // Leave as default/null if not present  
                            record.Add(field.Name, null);  
                        }  
                    }  
                    records.Add(record);  
                }  
            }  
        }  
  
        // 6. Serialize records to Avro binary  
        using var avroStream = new MemoryStream();  
        var writer = new BinaryEncoder(avroStream);  
  
        var genericWriter = new GenericWriter<GenericRecord>(avroSchema);  
  
        foreach (var rec in records)  
        {  
            genericWriter.Write(rec, writer);  
        }  
        avroStream.Seek(0, SeekOrigin.Begin);  
  
        // 7. POST the Avro binary stream  
        string streamUrl = $"{destination.TrimEnd('/')}/stream/{id}/{coll}";  
        using (var httpClient = new HttpClient())  
        using (var streamContent = new StreamContent(avroStream))  
        {  
            streamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");  
            try  
            {  
                Console.WriteLine($"Posting Avro stream to {streamUrl} ...");  
                var streamResponse = await httpClient.PostAsync(streamUrl, streamContent);  
                if (!streamResponse.IsSuccessStatusCode)  
                {  
                    Console.WriteLine($"Stream POST failed: {streamResponse.StatusCode} {streamResponse.ReasonPhrase}");  
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
  
        Console.WriteLine($"Done! Documents serialized and posted: {records.Count}");  
  
    },  
    sourceConnectionOption, collectionOption, schemaOption, destinationOption  
);  
  
return await rootCommand.InvokeAsync(args);  

