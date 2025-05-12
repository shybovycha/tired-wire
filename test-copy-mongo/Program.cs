using System.CommandLine;
using System.CommandLine.Binding;
using MongoDB.Bson;
using MongoDB.Driver;

var sourceConnectionOption = new Option<string>(
    "--source-db",
    () => "mongodb://localhost:27017",
    "Source MongoDB connection string"
)
{
    IsRequired = true,
};

var targetConnectionOption = new Option<string>(
    "--target-db",
    () => "mongodb://localhost:27017",
    "Target MongoDB connection string"
)
{
    IsRequired = true,
};

var collectionNameOption = new Option<string>(
    "--collection",
    description: "Name of the collection to copy"
)
{
    IsRequired = true,
};

var rootCommand = new RootCommand("Copy a MongoDB collection from one database to another")
{
    sourceConnectionOption,
    targetConnectionOption,
    collectionNameOption,
};

rootCommand.SetHandler(
    async (sourceDbUrl, targetDbUrl, collectionName) =>
    {
        var sourceClient = new MongoClient(sourceDbUrl);
        var targetClient = new MongoClient(targetDbUrl);

        var sourceDatabase = sourceClient.GetDatabase(
            new MongoUrlBuilder(sourceDbUrl).DatabaseName
        );

        var targetDatabase = targetClient.GetDatabase(
            new MongoUrlBuilder(targetDbUrl).DatabaseName
        );

        var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(collectionName);
        var targetCollection = targetDatabase.GetCollection<BsonDocument>(collectionName);

        using var cursor = await sourceCollection.FindAsync(FilterDefinition<BsonDocument>.Empty);

        int copied = 0;
        while (await cursor.MoveNextAsync())
        {
            var batch = cursor.Current.ToList();
            if (batch.Count > 0)
            {
                // Uncomment to remove _id to avoid duplicate keys:
                // batch.ForEach(doc => doc.Remove("_id"));
                await targetCollection.InsertManyAsync(batch);
                copied += batch.Count;
            }
        }
        Console.WriteLine($"Done! Documents copied: {copied}");
    },
    sourceConnectionOption,
    targetConnectionOption,
    collectionNameOption
);

return await rootCommand.InvokeAsync(args);
