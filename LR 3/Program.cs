using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using System.Configuration;

namespace MongoDBApp
{
    [BsonIgnoreExtraElements]
    public class Person
    {
        //[BsonRepresentation(BsonType.ObjectId)]
        public ObjectId Id { get; set; }
        //[BsonElement("First name")]
        public string Name { get; set; }
        //[BsonIgnore]
        public string Surname { get; set; }
        //[BsonIgnoreIfDefault]
        //[BsonRepresentation(BsonType.String)]
        public int Age { get; set; }
        //[BsonIgnoreIfNull]
        public Company Company { get; set; }
        //[BsonIgnoreIfNull]
        public List<string> Languages { get; set; }

    }
    public class Company
    {
        public string Name { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;
            string con = ConfigurationManager.ConnectionStrings["MongoDb"].ConnectionString;
            MongoClient client = new MongoClient(con);
            GetDatabasesNames(client).GetAwaiter();
            GetCollectionsNames(client).GetAwaiter();
            GetSpecificCollection(client).GetAwaiter();
            WorkingWithDocuments().GetAwaiter();
            WorkingWithModels().GetAwaiter();
            WorkingWithBsonClassMap().GetAwaiter();
            WorkingWithConventions().GetAwaiter();
            Console.ReadLine();
        }
        private static async Task GetDatabasesNames(MongoClient client)
        {
            using (var cursor = await client.ListDatabasesAsync())
            {
                var databaseDocuments = await cursor.ToListAsync();
                foreach (var databaseDocument in databaseDocuments)
                {
                    Console.WriteLine(databaseDocument["name"]);
                }
            }
        }
        private static async Task GetCollectionsNames(MongoClient client)
        {
            using (var cursor = await client.ListDatabasesAsync())
            {
                var dbs = await cursor.ToListAsync();
                foreach(var db in dbs)
                {
                    Console.WriteLine("В базі даних {0} містяться такі колекції:", db["name"]);
                    IMongoDatabase database = client.GetDatabase(db["name"].ToString());
                    using(var collCursor = await database.ListCollectionsAsync())
                    {
                        var colls = await collCursor.ToListAsync();
                        foreach(var col in colls)
                        {
                            Console.WriteLine(col["name"]);
                        }
                    }
                    Console.WriteLine();
                }
            }
        }
        private static async Task GetSpecificCollection(MongoClient client)
        {
            IMongoDatabase database = client.GetDatabase("kovalenko16KCompanies");
            IMongoCollection<BsonDocument> col = database.GetCollection<BsonDocument>("kovalenko16KUsers");
            Console.WriteLine(col);
        }
        private static async Task WorkingWithDocuments()
        {
            BsonDocument doc = new BsonDocument();
            Console.WriteLine(doc);

            doc = new BsonDocument { { "name", "Oleksandr" } };
            Console.WriteLine(doc);
            Console.WriteLine(doc["name"]);
            doc["name"] = "Oleksii";
            Console.WriteLine(doc.GetValue("name"));

            BsonElement bel = new BsonElement("name", "Oleksandr");
            doc = new BsonDocument(bel);
            Console.WriteLine(doc);

            doc = new BsonDocument();
            doc.Add(bel);
            Console.WriteLine(doc);

            doc = new BsonDocument
            {
                { "name", "Oleksandr" },
                { "surname", "Kovalenko" },
                { "age", new BsonInt32(23) },
                {
                    "company",
                    new BsonDocument
                    {
                        { "name", "NMU" },
                        { "year", new BsonInt32(1899) },
                        { "price", new BsonInt32(320000) }
                    }
                }
            };
            Console.WriteLine(doc);

            BsonDocument chemp = new BsonDocument();
            chemp.Add("countries", new BsonArray(new[] {"Бразилія","Аргентина","Німеччина","Нідерланди"}));
            chemp.Add("finished", new BsonBoolean(true));
            Console.WriteLine(chemp);
        }
        private static async Task WorkingWithModels()
        {
            Person p = new Person {Name = "Harry", Surname = "Bosch", Age = 56 };
            p.Company = new Company { Name = "LAPD" };
            Console.WriteLine(p.ToJson());
        }
        private static async Task WorkingWithBsonClassMap()
        {
            BsonClassMap.RegisterClassMap<Person>(cm =>
            {
                cm.AutoMap();
                cm.MapMember(p => p.Name).SetElementName("name");
            });
            Person person = new Person { Name = "Harry", Age = 56 };
            BsonDocument doc = person.ToBsonDocument();
            Console.WriteLine(doc);
        }
        private static async Task WorkingWithConventions()
        {
            var conventionPack = new ConventionPack();
            conventionPack.Add(new CamelCaseElementNameConvention());
            ConventionRegistry.Register("camelCase", conventionPack, t => true);
            Person person = new Person { Name = "Bilitz", Age = 50 };
            BsonDocument doc = person.ToBsonDocument();
            Console.WriteLine(doc);
        }
    }
}