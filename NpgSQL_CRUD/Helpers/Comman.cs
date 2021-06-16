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
    }
}
