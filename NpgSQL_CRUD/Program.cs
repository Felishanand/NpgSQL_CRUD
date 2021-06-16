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

            Console.ReadKey();
        }

        private static async Task CheckHistory()
        {
            Console.WriteLine("CRUD for Check Hisotry");           

            var checkHistories = new List<CheckHistory>()
            {
                new CheckHistory{AssetClass = "AC1", AssetName ="AN1", AssetType = "AT1", ClusterName = "CN1", CollectionTimeResult=true, EndDate=new DateTime(2021,06,16), StartDate=new DateTime(2021,06,16)},
                new CheckHistory{AssetClass = "AC2", AssetName ="AN2", AssetType = "AT2", ClusterName = "CN2", CollectionTimeResult=true, EndDate=new DateTime(2021,06,16), StartDate=new DateTime(2021,06,16)},
                new CheckHistory{AssetClass = "Ac3", AssetName ="AN2", AssetType = "AT2", ClusterName = "CN3", CollectionTimeResult=true, EndDate=new DateTime(2021,06,16), StartDate=new DateTime(2021,06,16)},
            };

            //CheckHistoryService.Insert_CheckHistory();

            var hasInserted = await CheckHistoryService.InsertCheckLists(checkHistories);

            if (hasInserted)
            {
                Console.WriteLine("check history inserted successfully...");
            }
            else
            {
                Console.WriteLine("Check History Inserted Failed...");
            }


            Console.WriteLine("Get Check History.\n");
            var list = await CheckHistoryService.GetCheckList();

            Console.WriteLine($"AssetClass \b\b AssetName \b\b AssetType\b\b ClusterName \b\b StartDate \b\b EndDate \b\b CollectionTimeResult \n");
            Console.WriteLine("..................................................................................................................");

            foreach (var checkhistory in list)
            {
                Console.Write($"{checkhistory.AssetClass}\b\b{checkhistory.AssetName} {checkhistory.AssetType} {checkhistory.ClusterName} {checkhistory.StartDate} {checkhistory.EndDate} {checkhistory.CollectionTimeResult}\n");
            }

            Console.WriteLine("..................................................................................................................");

           
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
     
    }
}