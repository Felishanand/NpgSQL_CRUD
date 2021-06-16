using System;
using System.Collections.Generic;
using Npgsql;

namespace NpgSQL_CRUD
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to NpgSQL CRUD Operation Using .Net Core 3.1 with Ado.net ");

            //TestConnection();
            Console.WriteLine("Insert a Student Record:");
            Insert_Student();

            Console.WriteLine("Student List:");
            var students = Get_Student();

            foreach (var student in students)
            {
                Console.WriteLine($"Name: {student.Name} Fees: {student.Fees} \n" );
            }

            Console.ReadKey();
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

                if (n==1)
                {
                    Console.WriteLine("Student Inserted Successfully");
                }
            }
        }

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

        private static NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection(@"Server=localhost;Port=5432;User Id=postgres;Password=Tn37cr!!83;Database=TestDB;");
        }

    }
}
