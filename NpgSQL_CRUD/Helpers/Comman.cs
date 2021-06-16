using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace NpgSQL_CRUD
{
    public class Comman
    {   
        public static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Server=localhost;Port=5432;User Id=postgres;Password=Tn37cr!!83;Database=TestDB;");
        }

        public static string GenerateString()
        {
            Random random = new Random();
            int length = 16;
            var rString = "";
            for (var i = 0; i < length; i++)
            {
                rString += ((char)(random.Next(1, 26) + 64)).ToString().ToLower();
            }

            return rString;

        }

        public static PostgreSQLCopyHelper<Student> CreateCopyHelper()
        {
            return new PostgreSQLCopyHelper<Student>("public", "students")
                .Map("Name", x => x.Name)
                .Map("Fees", x => x.Fees)
                .MapDate("EnrollmentDate", x => x.EnrollmentDate);
        }

        public static PostgreSQLCopyHelper<CheckHistory> CheckHistoryMapper()
        {            
            return new PostgreSQLCopyHelper<CheckHistory>("public", "check_history")
                .Map("asset_class", x => x.AssetClass)
                .Map("asset_name", x => x.AssetName)
                .Map("asset_type", x => x.AssetType)
                .Map("cluster_name", x => x.ClusterName)
                .Map("collection_time_result", x => x.CollectionTimeResult)
                .MapDate("end_date", x => x.EndDate)
                .MapDate("start_date", x => x.StartDate);
        }

        public static PostgreSQLCopyHelper<DayWiseAvailability> DayWiseAvailabilityMapper()
        {
            return new PostgreSQLCopyHelper<DayWiseAvailability>("public", "daywaise_availability")
                .Map("asset_class", x => x.AssetClass)
                .Map("asset_name", x => x.AssetName)
                .Map("asset_type", x => x.AssetType)
                .Map("cluster_name", x => x.ClusterName)
                .Map("availability_percentage", x => x.AvailabilityPercentage)
                .MapDate("collection_date", x => x.CollectionDate);                
        }
    }
}
