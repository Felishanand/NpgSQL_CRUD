using Npgsql;
using NpgSQL_CRUD;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NpgSQL_CRUD
{
    public class DayWiseAvailabilityService
    {
        private const string _tblDayWiseAvailabilityName = "daywaise_availability";

        public async static Task<bool> InsertOrUpdateDayWiseAvailabilities(List<DayWiseAvailability> dayWiseAvailabilities)
        {
            await using (var conn = Comman.GetConnection())
            {
                await conn.OpenAsync();

                foreach (DayWiseAvailability rule in dayWiseAvailabilities)
                {
                    try

                    {
                        NpgsqlCommand cmd = await InsertOrUpdateDayWiseAvailability(conn, rule);
                    }
                    catch (PostgresException ex)
                    {
                        if (ex.SqlState == "42P01") // table does not exist
                        {
                            Console.WriteLine($"Table {_tblDayWiseAvailabilityName} does not exists. Attempting to create.");

                            if (!await CreateDayWiseAvailability(conn))

                                throw new Exception("Error while creating table for Day Wise Availability");

                            Console.WriteLine($"Initialized table {_tblDayWiseAvailabilityName}");

                            Console.WriteLine($"Retry inserts into {_tblDayWiseAvailabilityName}");

                            await conn.CloseAsync();

                            return await InsertOrUpdateDayWiseAvailabilities(dayWiseAvailabilities);
                        }
                        else // unknown error
                        {
                            Console.WriteLine($"Failed to insert day wise availability: \n{ex}");

                            await conn.CloseAsync();

                            throw;
                        }
                    }
                    catch (Exception ex)

                    {
                        Console.WriteLine($"Failed to insert day wise availability: \n{ex}");

                        await conn.CloseAsync();

                        throw;
                    }
                }

                await conn.CloseAsync();
            }

            return true;
        }

        private async static Task<NpgsqlCommand> InsertOrUpdateDayWiseAvailability(NpgsqlConnection conn, DayWiseAvailability rule)
        {
            try
            {
                var cmd = new NpgsqlCommand($@"Insert into {_tblDayWiseAvailabilityName} (
                                                                    asset_name,
                                                                    cluster_name,
                                                                    asset_type,
                                                                    asset_class,
                                                                    collection_date,
                                                                    availability_percentage)
                                                               Values (
                                                                       @asset_name,
                                                                       @cluster_name,
                                                                       @asset_type,
                                                                       @asset_class,
                                                                       @collection_date,
                                                                       @availability_percentage
                                                                )
                                                               ON CONFLICT ON CONSTRAINT {_tblDayWiseAvailabilityName}_pkey DO UPDATE
                                                               SET asset_name = @asset_name,
                                                                   cluster_name = @cluster_name,
                                                                   asset_type = @asset_type,
                                                                   asset_class = @asset_class,
                                                                   collection_date = @collection_date,
                                                                   availability_percentage = @availability_percentage; ", conn);

                cmd.Parameters.AddWithValue("asset_name", rule.AssetName);
                cmd.Parameters.AddWithValue("cluster_name", rule.ClusterName);
                cmd.Parameters.AddWithValue("asset_type", rule.AssetType);
                cmd.Parameters.AddWithValue("asset_class", rule.AssetClass);
                cmd.Parameters.AddWithValue("collection_date", rule.CollectionDate);
                cmd.Parameters.AddWithValue("availability_percentage", rule.AvailabilityPercentage);

                Console.WriteLine($"Inserting day wise availability: " + Environment.NewLine +
                    $"{nameof(rule.AssetName)} - {rule.AssetName}" + Environment.NewLine +
                    $"{nameof(rule.ClusterName)} - {rule.ClusterName}" + Environment.NewLine +
                    $"{nameof(rule.AssetType)} - {rule.AssetType}" + Environment.NewLine +
                    $"{nameof(rule.AssetClass)} - {rule.AssetClass}" + Environment.NewLine +
                    $"{nameof(rule.CollectionDate)} - {rule.CollectionDate}" + Environment.NewLine +
                    $"{nameof(rule.AvailabilityPercentage)} - {rule.AvailabilityPercentage}");

                await cmd.ExecuteNonQueryAsync();

                Console.WriteLine($"Day wise Availablity {rule.AssetName} inserted successfully.");

                return cmd;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        //GetLast30daysAvailabity - Calculate start and end date - (end date - 30days)

        //return list of day avilabilty model

        public async static Task<List<DayWiseAvailability>> GetLast30DayWiseAvailability()
        {
            Console.WriteLine("Getting day wise availability");

            var list = new List<DayWiseAvailability>();

            await using (var conn = Comman.GetConnection())
            {
                await conn.OpenAsync();

                var collectionDate = DateTime.UtcNow.Date - DateTime.UtcNow.Date.AddDays(-30);

                await using (var cmd = new NpgsqlCommand($@" SELECT
                                                            asset_name,
                                                            cluster_name,
                                                            asset_type,
                                                            asset_class,
                                                            collection_date,
                                                            availability_percentage
                                                        FROM
                                                            {_tblDayWiseAvailabilityName}; ", conn))

                {
                    // read in results

                    await using var reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())

                    {
                        var rule = new DayWiseAvailability();

                        rule.AssetName = reader.GetString(0);

                        rule.ClusterName = reader.GetString(1);

                        rule.AssetType = reader.GetString(2);

                        rule.AssetClass = reader.GetString(3);

                        rule.CollectionDate = reader.GetDateTime(4);

                        rule.AvailabilityPercentage = reader.GetString(5);

                        list.Add(rule);
                    }
                }

                await conn.CloseAsync();
            }

            return list;
        }

        //ToDo:CreateDayWiseAvailability

        private async static Task<bool> CreateDayWiseAvailability(NpgsqlConnection conn)
        {
            try
            {
                await using var cmd = new NpgsqlCommand($@"CREATE Table {_tblDayWiseAvailabilityName} (
                                                            asset_name VARCHAR(128) NOT NULL,
                                                            cluster_name VARCHAR(128) NULL,
                                                            asset_type VARCHAR(128) NULL,
                                                            asset_class VARCHAR(128) NOT NULL,
                                                            collection_date DATE NOT NULL,
                                                            availability_percentage VARCHAR(128) NOT NULL,
                                                         PRIMARY KEY(cluster_name, asset_name, asset_class, collection_date)); ", conn);

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to create table {ex.Message}");

                return false;
            }

            return true;
        }

        //BulkInsert_Helper
        public static void BulkInsert_DayWiseAvailabilities(PostgreSQLCopyHelper<DayWiseAvailability> copyHelper, IEnumerable<DayWiseAvailability> entities)
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
    }
}