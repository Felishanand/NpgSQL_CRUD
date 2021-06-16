using Npgsql;
using PostgreSQLCopyHelper;
using System;
using System.Collections.Generic;
using System.Text;

namespace NpgSQL_CRUD
{
    public class StudentService
    {
        #region Students

        public static List<Student> Get_Student()
        {
            var students = new List<Student>();

            using (NpgsqlConnection conn = Comman.GetConnection())
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

        public static void Insert_Student()
        {
            using (NpgsqlConnection Conn = Comman.GetConnection())
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

        public static void BulkInsert_Students(PostgreSQLCopyHelper<Student> copyHelper, IEnumerable<Student> entities)
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

        #endregion Students       
    }
}
