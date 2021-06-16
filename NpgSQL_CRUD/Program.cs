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

        private static async Task Main(string[] args)
        {
            Console.WriteLine("Welcome to NpgSQL CRUD Operation Using .Net Core 3.1 with Ado.net ");

            #region Student

            //Student();

            #endregion Student

            #region CheckHistory

            await CheckHistory();

            #endregion

            #region DayWiseAvailabilities

            //await DayWiseAvailabilities();

            #endregion

            Console.ReadKey();
        }

        private static async Task CheckHistory()
        {
            Console.WriteLine("CRUD for Check Hisotry");           

            Console.WriteLine("No of Check History Inserting...");
            var noOfCheckHistory = Convert.ToInt32(Console.ReadLine());

            var checkHistories = new List<CheckHistory>();

            for (int i = 0; i < noOfCheckHistory; i++)
            {
                string assetclass = Comman.GenerateString();
                string assetname = Comman.GenerateString();
                string assettype = Comman.GenerateString();
                string clustername = Comman.GenerateString();                

                checkHistories.Add(new CheckHistory { AssetClass = assetclass, AssetName = assetname, AssetType = assettype, ClusterName = clustername, CollectionTimeResult = true, EndDate = new DateTime(2021, 06, 16), StartDate = new DateTime(2021, 06, 16) });
            }

            #region Insert

           // CheckHistoryService.Insert_CheckHistory();

            //var hasInserted = await CheckHistoryService.InsertCheckLists(checkHistories);

            //if (hasInserted)
            //{
            //    Console.WriteLine("check history inserted successfully...");
            //}
            //else
            //{
            //    Console.WriteLine("Check History Inserted Failed...");
            //}
            #endregion

            #region BulkInsert

            var res = CheckHistoryService.InsertCheckHistories(checkHistories);

            PostgreSQLCopyHelper<CheckHistory> copyHelper = Comman.CheckHistoryMapper();

            CheckHistoryService.BulkInsert_CheckHistorys(copyHelper, checkHistories);

            #endregion

            #region GetList

            Console.WriteLine("Get Check History.\n");
            var list = await CheckHistoryService.GetCheckList();

            Console.WriteLine($"AssetClass \b\b AssetName \b\b AssetType\b\b ClusterName \b\b StartDate \b\b EndDate \b\b CollectionTimeResult \n");
            Console.WriteLine("..................................................................................................................");

            foreach (var checkhistory in list)
            {
                Console.Write($"{checkhistory.AssetClass}\b\b{checkhistory.AssetName} {checkhistory.AssetType} {checkhistory.ClusterName} {checkhistory.StartDate} {checkhistory.EndDate} {checkhistory.CollectionTimeResult}\n");
            }

            Console.WriteLine("..................................................................................................................");

            #endregion
        }

        private static void Student()
        {
            Console.WriteLine("Bulk Inserts");

            var students = new List<Student>();

            Console.WriteLine("No of student enrolling...");
            var noOfStudent = Convert.ToInt32(Console.ReadLine());

            for (int i = 0; i < noOfStudent; i++)
            {
                string name = Comman.GenerateString();
                decimal fees = Convert.ToDecimal(i) * Convert.ToDecimal(5.25);
                DateTime enrollmentDate = DateTime.UtcNow.Date;

                students.Add(new Student { Name = name, Fees = fees, EnrollmentDate = enrollmentDate });
            }

            PostgreSQLCopyHelper<Student> copyHelper = Comman.CreateCopyHelper();

            StudentService.BulkInsert_Students(copyHelper, students);

            //TestConnection();

            Console.WriteLine("Insert a Student Record:");
            StudentService.Insert_Student();

            Console.WriteLine("Student List:");
            var result = StudentService.Get_Student();

            foreach (var student in result)
            {
                Console.WriteLine($"Name: {student.Name} Fees: {student.Fees} \n");
            }
        }

        private static void TestConnection()
        {
            using (NpgsqlConnection conn = Comman.GetConnection())
            {
                conn.Open();

                if (conn.State == System.Data.ConnectionState.Open)
                {
                    Console.WriteLine("Database Connected");
                }

                conn.Close();
            }
        }

        private static async Task DayWiseAvailabilities()
        {
            Console.WriteLine("CRUD for DayWiseAvailabilities");

            Console.WriteLine("No of DayWise Availabilities Inserting...");
            var noOfDayWiseAvailabilities = Convert.ToInt32(Console.ReadLine());

            var dayWiseAvailabilities = new List<DayWiseAvailability>();

            for (int i = 0; i < noOfDayWiseAvailabilities; i++)
            {
                string assetclass = Comman.GenerateString();
                string assetname = Comman.GenerateString();
                string assettype = Comman.GenerateString();
                string clustername = Comman.GenerateString();
                var availabilityPercentage = Convert.ToString(449784 / (i+1));

                dayWiseAvailabilities.Add(new DayWiseAvailability { AssetClass = assetclass, AssetName = assetname, AssetType = assettype, ClusterName = clustername, AvailabilityPercentage = availabilityPercentage, CollectionDate = new DateTime(2021, 06, 16)});
            }

            #region Insert            

            //var hasInserted = await DayWiseAvailabilityService.InsertOrUpdateDayWiseAvailabilities(dayWiseAvailabilities);

            //if (hasInserted)
            //{
            //    Console.WriteLine("Inserted successfully...");
            //}
            //else
            //{
            //    Console.WriteLine("Inserted Failed...");
            //}
            #endregion

            #region BulkInsert

            PostgreSQLCopyHelper<DayWiseAvailability> copyHelper = Comman.DayWiseAvailabilityMapper();

            DayWiseAvailabilityService.BulkInsert_DayWiseAvailabilities(copyHelper, dayWiseAvailabilities);

            #endregion

            #region GetList

            Console.WriteLine("Get Check History.\n");
            var list = await DayWiseAvailabilityService.GetLast30DayWiseAvailability();

            Console.WriteLine($"AssetClass \b\b AssetName \b\b AssetType\b\b ClusterName \b\b CollectionDate \b\b AvailabilityPercentage \n");
            Console.WriteLine("..................................................................................................................");

            foreach (var dayWiseAvailability in list)
            {
                Console.Write($"{dayWiseAvailability.AssetClass}\b\b{dayWiseAvailability.AssetName} {dayWiseAvailability.AssetType} {dayWiseAvailability.ClusterName} {dayWiseAvailability.CollectionDate} {dayWiseAvailability.AvailabilityPercentage}\n");
            }

            Console.WriteLine("..................................................................................................................");

            #endregion
        }

    }
}