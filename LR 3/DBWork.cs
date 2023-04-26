using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using MongoDBApp;

namespace LR_3
{
    class Employee
    {
        public string Name { set; get; }
        public int Age { set; get; }
    }
        public class DBWork
        {
            static string connectionString = "mongodb://localhost";
            static MongoClient client = new MongoClient(connectionString);
            static IMongoDatabase database = client.GetDatabase("test");
            static IGridFSBucket gridFS = new GridFSBucket(database);
            public static void LaunchWork()
            {
                SaveDocs().GetAwaiter().GetResult();
                FindAllDocs().GetAwaiter().GetResult();
                FindSpecificDocs().GetAwaiter().GetResult();
                SortPeople().GetAwaiter().GetResult();
                ProjectPeople().GetAwaiter().GetResult();
                UpdatePerson().GetAwaiter().GetResult();
                DeletePerson().GetAwaiter().GetResult();
                BulkOperation().GetAwaiter().GetResult();
                UploadFileAsync().GetAwaiter().GetResult();
                FindFileAsync().GetAwaiter().GetResult();
                ReplaceFileAsync().GetAwaiter().GetResult();
                DeleteFileAsync().GetAwaiter().GetResult();
                Console.ReadLine();
            }
            private static async Task SaveDocs()
            {
                var collection = database.GetCollection<BsonDocument>("people");
                BsonDocument person = new BsonDocument
            {
                {"Name", "Madison" },
                {"Age", 18 },
                {"Languages", new BsonArray{"english", "german"} },
                {"Company", new BsonDocument{ { "Name", "LAPD" } } }
            };
                await collection.InsertOneAsync(person);

                BsonDocument person1 = new BsonDocument
            {
                {"Name", "Honey" },
                {"Age", 52 },
                {"Languages", new BsonArray{"english", "german", "french"} },
				{"Company", new BsonDocument{ { "Name", "Private Investigation Agency" } } }
			};
                BsonDocument person2 = new BsonDocument
            {
                {"Name", "Cisco" },
                {"Age", 35 },
                {"Languages", new BsonArray{"french", "german"} },
				{"Company", new BsonDocument{ { "Name", "Private Investigation Agency" } } }
			};
                await collection.InsertManyAsync(new[] { person1, person2 });

                Person person3 = new Person
                {
                    Name = "Irwing",
                    Age = 40,
                    Languages = new List<string> { "english", "german" },
                    Company = new Company
                    {
                        Name = "LAPD"
                    }
                };
                await collection.InsertOneAsync(person3.ToBsonDocument());
            }
            public static async Task FindAllDocs()
            {
                var collection = database.GetCollection<BsonDocument>("people");
                var collection1 = database.GetCollection<Person>("people");
                var filter = new BsonDocument();
                using (var cursor = await collection.FindAsync(filter))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        var people1 = cursor.Current;
                        foreach (var doc in people1)
                        {
                            Console.WriteLine(doc);
                        }
                    }
                }

                var people = await collection.Find(filter).ToListAsync();
                foreach (var doc in people)
                {
                    Console.WriteLine(doc);
                }

                using (var cursor = await collection1.FindAsync(filter))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        var people2 = cursor.Current;
                        foreach (Person p in people2)
                        {
                            Console.WriteLine("{0} - {1} ({2})", p.Id, p.Name, p.Age);
                        }
                    }
                }

                var people3 = await collection1.Find(filter).ToListAsync();
                foreach (Person p in people3)
                {
                    Console.WriteLine("{0} - {1} ({2})", p.Id, p.Name, p.Age);
                }
            }
            public static async Task FindSpecificDocs()
            {
                var collection = database.GetCollection<BsonDocument>("people");
                var collection1 = database.GetCollection<Person>("people");
                //пошук записів, в яких Name = Harry
                Console.WriteLine("пошук записів, в яких Name = Harry");
                var filter = new BsonDocument("Name", "Harry");
                var people = await collection.Find(filter).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //пошук записів, в яких вік більше 30 років
                Console.WriteLine("пошук записів, в яких вік більше 30 років");
                filter = new BsonDocument("Age", new BsonDocument("$gt", 30));
                people = await collection.Find(filter).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //пошук записів, в яких вік більше або дорівнює 30 або Name = Madison
                Console.WriteLine("пошук записів, в яких вік більше або дорівнює 30 або Name = Madison");
                filter = new BsonDocument("$or", new BsonArray
            {
                new BsonDocument("Age", new BsonDocument("$gte",30)),
                new BsonDocument("Name", "Madison")
            });
                people = await collection.Find(filter).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //пошук записів, в яких вік більше 35 та Name = Irwing
                Console.WriteLine("пошук записів, в яких вік більше 35 та Name = Irwing");
                filter = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("Age", new BsonDocument("$gt",30)),
                new BsonDocument("Name", "Irwing")
            });
                people = await collection.Find(filter).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //пошук записів за властивістю об'єкта із посилання - Company name = LAPD
                Console.WriteLine("пошук записів за властивістю об'єкта із посилання - Company name = LAPD");
                var filter1 = Builders<BsonDocument>.Filter.Eq("Name", "LAPD");
                people = await collection.Find(filter1).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                var filter2 = Builders<Person>.Filter.Eq("Name", "LAPD");
                var people1 = await collection1.Find(filter2).ToListAsync();
                foreach (var p in people1)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Company.Name);
                }
                Console.WriteLine();
                //пошук записів за поєднанням кількох фільтрів
                Console.WriteLine("пошук записів, в яких Name = Honey або Name = Cisco");
                var builder = Builders<BsonDocument>.Filter;
                var filter3 = builder.Eq("Name", "Honey") | builder.Eq("Name", "Cisco");
                people = await collection.Find(filter3).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                Console.WriteLine("пошук записів, в яких Name = Honey та вік не дорівнює 56 рокам");
                filter3 = builder.Eq("Name", "Honey") & !builder.Eq("Age", 56);
                people = await collection.Find(filter3).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //використовуємо клас Person
                Console.WriteLine("пошук записів, в яких Name = Honey та вік не дорівнює 56 рокам");
                var people2 = await collection1.Find(x => x.Name == "Honey" && x.Age != 56).ToListAsync();
                foreach (var p in people2)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
                //пошук записів, в яких Name = Honey або вік 18 років
                Console.WriteLine("пошук записів, в яких Name = Honey або вік 18 років");
                var filter_1 = Builders<Person>.Filter.Eq("Name", "Honey");
                var filter_2 = Builders<Person>.Filter.Eq("Age", 18);
                var filterOr = Builders<Person>.Filter.Or(new List<FilterDefinition<Person>> { filter_1, filter_2 });
                var people_1 = await collection1.Find(filterOr).ToListAsync();
                foreach (var p in people_1)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
                //пошук записів, в яких Name = Madison та вік 18 років
                Console.WriteLine("пошук записів, в яких Name = Madison та вік 18 років");
                var filter_3 = Builders<Person>.Filter.Eq("Name", "Madison");
                var filter_4 = Builders<Person>.Filter.Eq("Age", 18);
                var filterAnd = Builders<Person>.Filter.Or(new List<FilterDefinition<Person>> { filter_3, filter_4 });
                var people_2 = await collection1.Find(filterAnd).ToListAsync();
                foreach (var p in people_2)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
                //пошук записів, в яких містяться англійська та німецька мови
                Console.WriteLine("пошук записів, в яких містяться англійська та німецька мови");
                var filter_lang = Builders<Person>.Filter.All("Languages", new List<string>() { "english", "german" });
                var people_3 = await collection1.Find(filterAnd).ToListAsync();
                foreach (var p in people_3)
                {
                Console.WriteLine("{0} - {1}", p.Name, string.Join(", ",p.Languages));
                }
                Console.WriteLine();
                //пошук записів, в яких містяться лише дві мови
                Console.WriteLine("пошук записів, в яких містяться лише дві мови");
                filter_lang = Builders<Person>.Filter.Size("Languages", 2);
                people_3 = await collection1.Find(filterAnd).ToListAsync();
                foreach (var p in people_3)
                {
                    Console.WriteLine("{0} - {1}", p.Name, string.Join(", ", p.Languages));
                }
                Console.WriteLine();
            }
            public static async Task SortPeople()
            {
                //сортування записів за зростанням віку
                Console.WriteLine("сортування записів за зростанням віку");
                var collection = database.GetCollection<BsonDocument>("people");
                var people = await collection.Find(new BsonDocument()).Sort("{Age:1}").ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p.ToString());
                }
                Console.WriteLine();
                var collection1 = database.GetCollection<Person>("people");
                //сортування за зменшенням значення поля Name
                Console.WriteLine("сортування за зменшенням значення поля Name");
                var result = await collection1.Find(new BsonDocument()).SortByDescending(e => e.Name).ToListAsync();
                foreach (var p in result)
                {
                    Console.WriteLine(p.Name);
                }
                Console.WriteLine();
                //сортування за зростанням значення поля Age
                Console.WriteLine("сортування за зростанням значення поля Age");
                var result1 = await collection1.Find(new BsonDocument()).SortBy(e => e.Age).ToListAsync();
                foreach (var p in result1)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
                //сортування за кількома ключами - за зростанням Company.Name та зменшенням Age
                Console.WriteLine("сортування за кількома ключами - за зростанням Company.Name та зменшенням Age");
                var sort = Builders<Person>.Sort.Ascending("Company.Name").Descending("Age");
                var result_comb = await collection1.Find(new BsonDocument()).Sort(sort).ToListAsync();
                foreach (var p in result_comb)
                {
                    Console.WriteLine("{0} - {1} - {2}", p.Name, p.Age, p.Company.Name);
                }
                Console.WriteLine();
                //пропуск перших двох документів із загальної кількості та виведення трьох наступних
                Console.WriteLine("пропуск перших двох документів із загальної кількості та виведення трьох наступних");
                var filter = new BsonDocument();
                var result_part = await collection.Find(filter).Skip(2).Limit(3).ToListAsync();
                foreach (var p in result_comb)
                {
                    Console.WriteLine(p.Name);
                }
                Console.WriteLine();
                //обчислення кількості документів у вибірці
                Console.WriteLine("обчислення кількості документів у вибірці");
                filter = new BsonDocument();
                long number = await collection.Find(filter).CountDocumentsAsync();
                Console.WriteLine(number);
                Console.WriteLine();
            }
            public static async Task ProjectPeople()
            {
                Console.WriteLine("Робота з проекціями");
                var collection = database.GetCollection<BsonDocument>("people");
                var people = await collection.Find(new BsonDocument()).Project("{Name:1, Age:1, _id:0}").ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                people = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("Name").Include("Age").Exclude("_id")).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //проекція з одного класу в інший
                Console.WriteLine("проекція з одного класу в інший\n");
                var collection1 = database.GetCollection<Person>("people");
                var filter = new BsonDocument();
                var projection = Builders<Person>.Projection.Expression(p => new Employee { Name = p.Name, Age = p.Age });
                var employees = await collection1.Find(filter).Project<Employee>(projection).ToListAsync();
                foreach (var e in employees)
                {
                    Console.WriteLine("{0} - {1}", e.Name, e.Age);
                }
                Console.WriteLine();
                //угрупування
                Console.Write("угрупування\n");
                people = await collection.Aggregate().Group(new BsonDocument { { "_id", "$Age" }, { "count", new BsonDocument("$sum", 1) } }).ToListAsync();
                foreach(BsonDocument p in people)
                {
                    Console.WriteLine("{0} - {1}", p.GetValue("_id"), p.GetValue("count"));
                }
                Console.WriteLine();
                //фільтрація даних
                Console.WriteLine("фільтрація даних за полем Name = Irwing, Company.Name = LAPD");
                var people1 = await collection1.Aggregate().Match(new BsonDocument { { "Name", "Irwing" }, { "Company.Name", "LAPD" } }).ToListAsync();
                foreach (Person p in people1)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
                //поєднання фільтрації та угрупування
                Console.WriteLine("поєднання фільтрації та угрупування");
                people = await collection.Aggregate().Match(new BsonDocument { { "Name", "Irwing"} })
                    .Group(new BsonDocument { { "_id", "$Company.Name" }, { "count", new BsonDocument("$sum", 1)} }).ToListAsync();
                foreach (BsonDocument p in people)
                {
                    Console.WriteLine("{0} - {1}", p.GetValue("_id"), p.GetValue("count"));
                }
                Console.WriteLine();
            }
            public static async Task UpdatePerson()
            {
                Console.WriteLine("Оновлення записів");
                var collection = database.GetCollection<BsonDocument>("people");
                var result = await collection.ReplaceOneAsync(new BsonDocument("Name", "Irwing"),
                    new BsonDocument
                    {
                        {"Name", "Irwin Irwing" },
                        {"Age", 45 },
                        {"Languages", new BsonArray(new[]{"english", "german"}) },
                        {"Company", new BsonDocument{{"Name", "LAPD"} } }
                    });
                Console.WriteLine("Знайдено за відповідністю: {0}; оновлено {1}", result.MatchedCount, result.ModifiedCount);
                var people = await collection.Find(new BsonDocument()).ToListAsync();
                foreach (var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //оновлення із додаванням до колекції
                Console.WriteLine("оновлення із додаванням до колекції");
                result = await collection.ReplaceOneAsync(new BsonDocument("Name", "Harry"),
                    new BsonDocument
                    {
                        {"Name","Harry Bosch" },
                        {"Age", 56 },
                        {"Languages", new BsonArray(new[]{"english"}) },
                        {"Company", new BsonDocument{{"Name", "LAPD"}} }
                    },
                    new UpdateOptions { IsUpsert = true });
                Console.WriteLine("Id доданого об'єкту: {0}", result.UpsertedId);
                //оновлення окремого поля
                Console.WriteLine("Оновлення окремого поля");
                var result1 = await collection.UpdateOneAsync(new BsonDocument("Name", "Madison"),
                    new BsonDocument("$set", new BsonDocument("Age", 20)));
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result1.MatchedCount, result1.ModifiedCount);
                Console.WriteLine();
                //збільшення числового значення
                Console.WriteLine("збільшення числового значення");
                var result_inc = await collection.UpdateOneAsync(
                    new BsonDocument("Name", "Harry"),
                    new BsonDocument("$inc", new BsonDocument("Age", 2)));
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result_inc.MatchedCount, result_inc.ModifiedCount);
                Console.WriteLine();
                //оновлення за допомогою UpdateDefinitionBuilder
                Console.WriteLine("оновлення за допомогою UpdateDefinitionBuilder");
                var filter = Builders<BsonDocument>.Filter.Eq("Name", "Cisco");
                var update = Builders<BsonDocument>.Update.Set("Age", 35);
                var result_udb = await collection.UpdateOneAsync(filter, update);
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result_udb.MatchedCount, result_udb.ModifiedCount);
                Console.WriteLine();
                //оновлення даних вкладених об'єктів
                Console.WriteLine("оновлення даних вкладених об'єктів");
                var filter1 = Builders<BsonDocument>.Filter.Eq("Company.Name", "LAPD");
                var update1 = Builders<BsonDocument>.Update.Set("Company.Name", "Los Angeles Police Department");
                var result2 = await collection.UpdateOneAsync(filter1, update1);
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result2.MatchedCount, result2.ModifiedCount);
                Console.WriteLine();
                //використання множинних критеріїв при оновленні
                Console.WriteLine("використання множинних критеріїв при оновленні");
                var builder = Builders<BsonDocument>.Filter;
                var filter2 = builder.Eq("Name", "Cisco") & builder.Eq("Age", 35);
                var update2 = Builders<BsonDocument>.Update.Set("Age", 33);
                var result3 = await collection.UpdateOneAsync(filter2, update2);
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result3.MatchedCount, result3.ModifiedCount);
                Console.WriteLine();
                var builder1 = Builders<BsonDocument>.Filter;
                var filter3 = builder.Eq("Name", "Cisco") | builder.Eq("Name", "Madison");
                var update3 = Builders<BsonDocument>.Update.Set("Age", 25);
                var result4 = await collection.UpdateOneAsync(filter3, update3);
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result4.MatchedCount, result4.ModifiedCount);
                Console.WriteLine();
                //додавання додаткового параметру - дати останньої зміни 
                Console.WriteLine("додавання додаткового параметру - дати останньої зміни");
                var filter4 = Builders<BsonDocument>.Filter.Eq("Name", "Honey");
                var update4 = Builders<BsonDocument>.Update.Set("Age", 48).CurrentDate("LastModified");
                var result5 = await collection.UpdateOneAsync(filter4, update4);
                //оновлення об'єктів типу Person
                Console.WriteLine("оновлення об'єктів типу Person");
                var collection1 = database.GetCollection<Person>("people");
                var filter5 = Builders<Person>.Filter.Eq("Name", "Harry");
                var update5 = Builders<Person>.Update.Set(x => x.Age, 56);
                var result6 = await collection1.UpdateManyAsync(filter5, update5);
                Console.WriteLine("знайдено за відповідністю: {0}; оновлено: {1}",
                    result6.MatchedCount, result6.ModifiedCount);
                var people1 = await collection1.Find(new BsonDocument()).ToListAsync();
                foreach(var p in people1)
                {
                    Console.WriteLine("{0} - {1}", p.Name, p.Age);
                }
                Console.WriteLine();
            }
            public static async Task DeletePerson()
            {
                Console.WriteLine("видалення записів за умовою Name = Harry Bosch");
                var collection = database.GetCollection<BsonDocument>("people");
                var filter = Builders<BsonDocument>.Filter.Eq("Name", "Harry Bosch");
                await collection.DeleteOneAsync(filter);
                var people = await collection.Find(new BsonDocument()).ToListAsync();
                foreach(var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //видалення записів за допомогою типу Person
                Console.WriteLine("видалення записів за умовою Name = Irwin Irwing");
                var collection1 = database.GetCollection<Person>("people");
                var result = await collection1.DeleteManyAsync(p => p.Name == "Irwin Irwing");
                Console.WriteLine("видалено: {0}", result.DeletedCount);
                Console.WriteLine();
            }
            public static async Task BulkOperation()
            {
                Console.WriteLine("видалення даних за допомогою BulkOperation");
                var collection = database.GetCollection<BsonDocument>("people");
                var result = await collection.BulkWriteAsync(new WriteModel<BsonDocument>[]
                {
                    new DeleteOneModel<BsonDocument>(new BsonDocument("Name", "Honey"))
                });
                Console.WriteLine("видалено: {0}", result.DeletedCount);
                var people = await collection.Find(new BsonDocument()).ToListAsync();
                foreach(var p in people)
                {
                    Console.WriteLine(p);
                }
                Console.WriteLine();
                //використання набору операцій
                Console.WriteLine("використання набору операцій за допомогою BulkOperation");
                var filter = Builders<BsonDocument>.Filter.Eq("Company.Name", "Los Angeles Police Department");
                var update = Builders<BsonDocument>.Update.Set("Company.Name", "LAPD");
                var newDoc = new BsonDocument
                {
                    {"Name","Barrel" },
                    {"Age", 67 },
                    {"Languages", new BsonArray{"english","german"} },
                    {"Company", new BsonDocument{{"Name", "LAPD"}} }
                };
                var result1 = await collection.BulkWriteAsync(new WriteModel<BsonDocument>[]
                    {
                    new DeleteOneModel<BsonDocument>(new BsonDocument("Name", "Cisco")),
                        new UpdateManyModel<BsonDocument>(filter,update),
                        new InsertOneModel<BsonDocument>(newDoc)
                });
                Console.WriteLine("видалено: {0}; додано: {1}; оновлено: {2}", 
                    result1.DeletedCount, result1.InsertedCount, result1.ModifiedCount);
            }
            public static async Task UploadFileAsync()
            {
			Console.WriteLine("Завантаження файлу до бази даних");
			//Завантаження файлу до GridFS
			using (Stream fs = new FileStream("D:\\Study\\NRDB\\Fritz_Teufel.jpg", FileMode.Open))
                {
                    ObjectId id = await gridFS.UploadFromStreamAsync("Fritz_Teufel.jpg", fs);
                    Console.WriteLine("Id об'єкту: {0}", id.ToString());
                }
            }
            public static async Task FindFileAsync()
            {
            Console.WriteLine("Пошук файлу в базі даних");
                //завантаження файлу із GridFS
                using (Stream fs = new FileStream("D:\\Study\\NRDB\\kitty.jpg", FileMode.OpenOrCreate))
                {
                    await gridFS.DownloadToStreamByNameAsync("Fritz_Teufel.jpg", fs);
                }
                //пошук файлів
                var filter = Builders<GridFSFileInfo>.Filter.Eq<string>(info => info.Filename, "Fritz_Teufel.jpg");
                var fileInfos = await gridFS.FindAsync(filter);
                var fileInfo = fileInfos.FirstOrDefault();
                Console.WriteLine("id = {0}", fileInfo.Id);
            }
            public static async Task ReplaceFileAsync()
            {
			Console.WriteLine("Заміна файлу в базі даних");
			using (Stream fs = new FileStream ("D:\\Study\\NRDB\\MR.Teufel.jpg",FileMode.Open))
                {
                    await gridFS.UploadFromStreamAsync("Fritz_Teufel.jpg", fs,
                        new GridFSUploadOptions { Metadata = new BsonDocument ("filename", "Fritz_Teufel.jpg") });
                }
            }
            public static async Task DeleteFileAsync()
            {
			Console.WriteLine("Видалення файлу з бази даних");
			var builder = new FilterDefinitionBuilder<GridFSFileInfo>();
                var filter = Builders<GridFSFileInfo>.Filter.Eq<string>(info => info.Filename, "Fritz_Teufel.jpg");
                var fileInfos = await gridFS.FindAsync(filter);
                var fileInfo = fileInfos.FirstOrDefault();
                await gridFS.DeleteAsync(fileInfo.Id);
            }
        }
}
