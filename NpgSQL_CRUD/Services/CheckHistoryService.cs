using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace NpgSQL_CRUD
{
    public class CheckHistoryService
    {
        private const string _tblCheckHistoryName = "check_history";
        private const string _tblDayWiseAvailabilityName = "daywaise_availability";
        public async static Task<List<CheckHistory>> GetCheckList()

        {
            Console.WriteLine("Getting check history list");

            var list = new List<CheckHistory>();

            await using (var conn = Comman.GetConnection())
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

        public static void Insert_CheckHistory()
        {
            using (NpgsqlConnection Conn = Comman.GetConnection())
            {
                Console.WriteLine("\nEnter a AssetName:");
                var asset_name = Console.ReadLine();

                Console.WriteLine("\nEnter a ClusterName:");
                var cluster_name = Console.ReadLine();

                Console.WriteLine("\nEnter a AssetType:");
                var asset_type = Console.ReadLine();

                Console.WriteLine("\nEnter a AssetClass:");
                var asset_class = Console.ReadLine();

                //Console.WriteLine("\nEnter a StartDate:");
                var start_date = new DateTime(2021,06,16);

                //Console.WriteLine("\nEnter a EndDate:");
                var end_date = new DateTime(2021, 06, 16);

                //Console.WriteLine("\nEnter a CollectionTimeResult:");
                var collection_time_result = true;

                string query = $@"INSERT INTO {_tblCheckHistoryName}
                                        (asset_name,cluster_name,asset_type,asset_class,start_date,end_date,collection_time_result)
                                  VALUES
                                        (@asset_name,@cluster_name,@asset_type,@asset_class,@start_date,@end_date,@collection_time_result)";

                NpgsqlCommand cmd = new NpgsqlCommand(query, Conn);

                cmd.Parameters.AddWithValue("asset_name", asset_name);
                cmd.Parameters.AddWithValue("cluster_name", cluster_name);
                cmd.Parameters.AddWithValue("asset_type", asset_type);
                cmd.Parameters.AddWithValue("asset_class", asset_class);
                cmd.Parameters.AddWithValue("start_date", start_date);
                cmd.Parameters.AddWithValue("end_date", end_date);
                cmd.Parameters.AddWithValue("collection_time_result", collection_time_result);

                Conn.Open();

                int n = cmd.ExecuteNonQuery();

                if (n == 1)
                {
                    Console.WriteLine("CheckHistory Inserted Successfully");
                }
            }
        }

        public static void BulkInsert_CheckHistorys(PostgreSQLCopyHelper<CheckHistory> copyHelper, IEnumerable<CheckHistory> entities)
        {
            try
            {
                using (var connection = Comman.GetConnection())
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

        public async static Task<bool> InsertCheckLists(List<CheckHistory> checkHitories)
        {
            await using (var conn = Comman.GetConnection())
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
                                                                        @start_date,
                                                                        @end_date,
                                                                        @collection_time_result
                                                                )
                                                               ON CONFLICT ON CONSTRAINT {_tblCheckHistoryName}_pkey DO UPDATE
                                                               SET
                                                                   asset_name = @asset_name,
                                                                   cluster_name = @cluster_name,
                                                                   asset_type = @asset_type,
                                                                   asset_class = @asset_class,
                                                                   start_date = @start_date,
                                                                   end_date = @end_date,
                                                                   collection_time_result = @collection_time_result; ", conn);

                cmd.Parameters.AddWithValue("asset_name", checkHistory.AssetName);
                cmd.Parameters.AddWithValue("cluster_name", checkHistory.ClusterName);
                cmd.Parameters.AddWithValue("asset_type", checkHistory.AssetType);
                cmd.Parameters.AddWithValue("asset_class", checkHistory.AssetClass);
                cmd.Parameters.AddWithValue("start_date", checkHistory.StartDate);
                cmd.Parameters.AddWithValue("end_date", checkHistory.EndDate);
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
    }
}
