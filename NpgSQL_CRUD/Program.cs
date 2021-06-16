using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NpgSQL_CRUD
{
    internal class Program
    {
        private const string _tblCheckHistoryName = "check_history";
        private const string _tblDayWiseAvailabilityName = "daywaise_availability";

        private static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Server=localhost;Port=5432;User Id=postgres;Password=Tn37cr!!83;Database=TestDB;");
        }

        private static async Task Main(string[] args)
        {
            Console.WriteLine("Welcome to NpgSQL CRUD Operation Using .Net Core 3.1 with Ado.net ");

            #region Student

            Console.WriteLine("Bulk Inserts");

            var students = new List<Student>();

            Console.WriteLine("No of student enrolling...");
            var noOfStudent = Convert.ToInt32(Console.ReadLine());

            for (int i = 0; i < noOfStudent; i++)
            {
                string name = $"Student {i}";
                decimal fees = Convert.ToDecimal(i) * Convert.ToDecimal(5.25) ;

                students.Add(new Student { Name = name, Fees = fees });
            }

            PostgreSQLCopyHelper<Student> copyHelper = CreateCopyHelper();
            
            BulkInsert_Students(copyHelper, students);
           
            //TestConnection();

            //Console.WriteLine("Insert a Student Record:");
            //Insert_Student();

            //Console.WriteLine("Student List:");
            //var students = Get_Student();

            //foreach (var student in students)
            //{
            //    Console.WriteLine($"Name: {student.Name} Fees: {student.Fees} \n");
            //}

            #endregion Student

            #region CheckHistory

            //Console.WriteLine("CRUD for Check Hisotry");

            //Console.WriteLine("Get Check History.\n");
            //var list = await GetCheckList();

            //foreach (var checkhistory in list)
            //{
            //    Console.Write($"{checkhistory.AssetClass} {checkhistory.AssetName} {checkhistory.AssetType} {checkhistory.ClusterName} {checkhistory.CollectionTimeResult}\n");
            //}

            //var checkHistories = new List<CheckHistory>()
            //{
            //    new CheckHistory{AssetClass = "AC1", AssetName ="AN1", AssetType = "AT1", ClusterName = "CN1", CollectionTimeResult=true, EndDate=DateTime.UtcNow, StartDate=DateTime.Now, Id =Guid.NewGuid()},
            //    new CheckHistory{AssetClass = "AC2", AssetName ="AN2", AssetType = "AT2", ClusterName = "CN2", CollectionTimeResult=true, EndDate=DateTime.UtcNow, StartDate=DateTime.Now, Id =Guid.NewGuid()},
            //    new CheckHistory{AssetClass = "Ac3", AssetName ="AN2", AssetType = "AT2", ClusterName = "CN3", CollectionTimeResult=true, EndDate=DateTime.UtcNow, StartDate=DateTime.Now, Id =Guid.NewGuid()},
            //};

            //var hasInserted = await InsertCheckLists(checkHistories);

            //if (hasInserted)
            //{
            //    Console.WriteLine("check history inserted successfully...");
            //}
            //else
            //{
            //    Console.WriteLine("Check History Inserted Failed...");
            //}

            #endregion

            Console.ReadKey();
        }

        private static void TestConnection()
        {
            using (NpgsqlConnection conn = GetConnection())
            {
                conn.Open();

                if (conn.State == System.Data.ConnectionState.Open)
                {
                    Console.WriteLine("Database Connected");
                }

                conn.Close();
            }
        }

        #region Students

        private static List<Student> Get_Student()
        {
            var students = new List<Student>();

            using (NpgsqlConnection conn = GetConnection())
            {
                conn.Open();

                string query = @"Select Name,Fees from public.Students";

                NpgsqlCommand cmd = new NpgsqlCommand(query, conn);

                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    var student = new Student();

                    student.Name = reader.GetString(0);
                    student.Fees = reader.GetDecimal(1);

                    students.Add(student);
                }
            }

            return students;
        }

        private static void Insert_Student()
        {
            using (NpgsqlConnection Conn = GetConnection())
            {
                Console.WriteLine("Enter a name:");
                var name = Console.ReadLine();

                Console.WriteLine("\n Enter a fees amount:");
                var fees = Convert.ToDecimal(Console.ReadLine());

                string query = $@"INSERT INTO Public.Students(Name,Fees) VALUES
                                        (@name,@fees)";

                NpgsqlCommand cmd = new NpgsqlCommand(query, Conn);

                cmd.Parameters.AddWithValue("name", name);
                cmd.Parameters.AddWithValue("fees", fees);

                Conn.Open();

                int n = cmd.ExecuteNonQuery();

                if (n == 1)
                {
                    Console.WriteLine("Student Inserted Successfully");
                }
            }
        }

        private static PostgreSQLCopyHelper<Student> CreateCopyHelper()
        {
            return new PostgreSQLCopyHelper<Student>("public", "students")
                .Map("Name", x => x.Name)
                .Map("Fees", x => x.Fees);
        }

        private static void BulkInsert_Students(PostgreSQLCopyHelper<Student> copyHelper, IEnumerable<Student> entities)
        {
            try
            {
                using (var connection = GetConnection())
                {
                    connection.Open();

                    copyHelper.SaveAll(connection, entities);

                    Console.WriteLine("Successfully Inserted");
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error {ex}");
            }

        }
      
        #endregion Students       
       
        #region CheckHistory

        //ToDo: CreateCheckHistoryTable

        public async static Task<List<CheckHistory>> GetCheckList()

        {
            Console.WriteLine("Getting check history list");

            var list = new List<CheckHistory>();

            await using (var conn = GetConnection())
            {
                await conn.OpenAsync();

                await using (var cmd = new NpgsqlCommand($@" SELECT

                                                            asset_name,

                                                            cluster_name,

                                                            asset_type,

                                                            asset_class,

                                                            start_date,

                                                            end_date,

                                                            collection_time_result

                                                        FROM

                                                            {_tblCheckHistoryName}; ", conn))

                {
                    // read in results

                    await using var reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())

                    {
                        var checkHistory = new CheckHistory();

                        checkHistory.AssetName = reader.GetString(0);

                        checkHistory.ClusterName = reader.GetString(1);

                        checkHistory.AssetType = reader.GetString(2);

                        checkHistory.AssetClass = reader.GetString(3);

                        checkHistory.StartDate = reader.GetDateTime(4);

                        checkHistory.EndDate = reader.GetDateTime(5);

                        checkHistory.CollectionTimeResult = reader.GetBoolean(6);

                        list.Add(checkHistory);
                    }
                }

                await conn.CloseAsync();
            }

            return list;
        }

        public async static Task<bool> InsertCheckLists(List<CheckHistory> checkHitories)
        {
            await using (var conn = GetConnection())
            {
                await conn.OpenAsync();

                foreach (CheckHistory checkHistory in checkHitories)
                {
                    try
                    {
                        NpgsqlCommand cmd = await InsertCheckList(conn, checkHistory);
                    }
                    catch (PostgresException ex)
                    {
                        if (ex.SqlState == "42601") // table does not exist
                        {
                            Console.WriteLine($"Table {_tblCheckHistoryName} does not exists. Attempting to create.");

                            if (!await CreateCheckHistoryTable(conn))
                                throw new Exception("Error while creating table for Check History");

                            Console.WriteLine($"Initialized table {_tblCheckHistoryName}");

                            Console.WriteLine($"Retry inserts into {_tblCheckHistoryName}");

                            await conn.CloseAsync();

                            return await InsertCheckLists(checkHitories);
                        }
                        else // unknown error
                        {
                            Console.WriteLine($"Failed to insert check history: \n{ex.Message}");

                            await conn.CloseAsync();

                            throw;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to insert check history: \n{ex.Message}");

                        await conn.CloseAsync();

                        throw;
                    }
                }

                await conn.CloseAsync();
            }

            return true;
        }

        private async static Task<bool> CreateCheckHistoryTable(NpgsqlConnection conn)
        {
            try
            {
                //toDo: Start and End time
                await using var cmd = new NpgsqlCommand($@"CREATE Table {_tblCheckHistoryName} (
                                                            asset_name VARCHAR(128) NOT NULL,
                                                            cluster_name VARCHAR(128) NULL,
                                                            asset_type VARCHAR(128) NULL,
                                                            asset_class VARCHAR(128) NOT NULL,
                                                            start_date Date NOT NULL,
                                                            end_date Date NOT NULL,
                                                            collection_time_result BOOLEAN NOT NULL,
                            PRIMARY KEY(cluster_name, asset_name, asset_class, start_date, end_date)); ", conn);

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create table {ex.Message}");
                return false;
            }

            return true;
        }

        //toDo: GetCheckList
        //toDo: Insertcheckhistory

        //Batch insert
        private async static Task<NpgsqlCommand> InsertCheckList(NpgsqlConnection conn, CheckHistory checkHistory)
        {
            try
            {
                var cmd = new NpgsqlCommand($@"Insert into {_tblCheckHistoryName} (
                                                                     asset_name,
                                                                     cluster_name,
                                                                     asset_type,
                                                                     asset_class,
                                                                     start_date,
                                                                     end_date,
                                                                     collection_time_result)
                                                               Values ( @asset_name,
                                                                        @cluster_name,
                                                                        @asset_type,
                                                                        @asset_class,
                                                                        now()::date,
                                                                        now()::date,
                                                                        @collection_time_result
                                                                )
                                                               ON CONFLICT ON CONSTRAINT {_tblCheckHistoryName}_pkey DO UPDATE
                                                               SET
                                                                   asset_name = @asset_name
                                                                   cluster_name = @cluster_name,
                                                                   asset_type = @asset_type,
                                                                   asset_class = @asset_class,
                                                                   start_date = @start_date,
                                                                   end_date = @end_date,
                                                                   collection_time_result = @collection_time_result; ", conn);

                var startDate =new DateTime(2021,06,16); //now()::date;
                var endDate = new DateTime(2021, 06, 16);

                    cmd.Parameters.AddWithValue("asset_name", checkHistory.AssetName);
                cmd.Parameters.AddWithValue("cluster_name", checkHistory.ClusterName);
                cmd.Parameters.AddWithValue("asset_type", checkHistory.AssetType);
                cmd.Parameters.AddWithValue("asset_class", checkHistory.AssetClass);
                //cmd.Parameters.AddWithValue("start_date", "2021-06-16");
                //cmd.Parameters.AddWithValue("end_date", "2021-06-16");
                cmd.Parameters.AddWithValue("collection_time_result", checkHistory.CollectionTimeResult);

                Console.WriteLine($"Inserting alert rule: " + Environment.NewLine +
                    $"{nameof(checkHistory.AssetName)} - {checkHistory.AssetName}" + Environment.NewLine +
                    $"{nameof(checkHistory.ClusterName)} - {checkHistory.ClusterName}" + Environment.NewLine +
                    $"{nameof(checkHistory.AssetType)} - {checkHistory.AssetType}" + Environment.NewLine +
                    $"{nameof(checkHistory.AssetClass)} - {checkHistory.AssetClass}" + Environment.NewLine +
                    $"{nameof(checkHistory.StartDate)} - {checkHistory.StartDate}" + Environment.NewLine +
                    $"{nameof(checkHistory.EndDate)} - {checkHistory.EndDate}" + Environment.NewLine +
                    $"{nameof(checkHistory.CollectionTimeResult)} - {checkHistory.CollectionTimeResult}");

                await cmd.ExecuteNonQueryAsync();

                Console.WriteLine($"Check History {checkHistory.AssetName} inserted successfully.");

                return cmd;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
            
        }

        #endregion CheckHistory
    }
}