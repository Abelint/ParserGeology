using System.Text;

using System.Data.SQLite;
using System.Data.Common;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;
using System.Diagnostics;
using System;

namespace ParserGeology
{
    internal class Program
    {
        //668 x 23 операций х секунды без потоков
        static Stopwatch stopwatch;
        static int bigCount = 0;
        static void Main(string[] args)
        {

            long freq = Stopwatch.Frequency; //частота таймера
            stopwatch = new Stopwatch();
            stopwatch.Start();

            string path = "opendata.csv";

           
            string firstLine = File.ReadLines(path).ElementAtOrDefault(0);
            string[] col2 = firstLine.Split(';');
            string[] col = new string[col2.Length];
            for(int i = 0; i < col2.Length; i++)
            {
                col2[i] = col2[i].Trim().Replace(' ', '_').Replace('/', '_').Replace(',', '_');
                col[i] = col2[i] + " TEXT";
            }
            string namedb = "Parser";
            string tableName = "Parse";
            string[] CK = { "ГСК-2011", "СК-42", "noName" };
           
            CreateDB(namedb);

            string connectionString = "Data Source=" + namedb + ".sqlite;Version=3;";
            SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString);
            m_dbConnection.Open();

            CreateTableDB(m_dbConnection, namedb, tableName, col, CK);


            int count = 0;
            using (StreamReader reader = new StreamReader(path))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    count++;
                    if (count == 1) continue;
                    
                    AddToTableDB(m_dbConnection, namedb, tableName, col2,line);

                }
            }
          
          
            List<string> gcndb =  GetColumnNameDB(m_dbConnection, namedb, tableName);
           foreach(string gcn in gcndb)
            {
                Console.WriteLine(gcn);
            }
            
            m_dbConnection.Close();

            stopwatch.Stop();
            double sec = (double)stopwatch.ElapsedTicks / freq; //переводим такты в секунды
            Console.WriteLine($"Частота таймера {freq} такт/с \r\n Время в тактах {stopwatch.ElapsedTicks} \r\n Время в секундах {sec}");
        }

        private static List<string> GetColumnNameDB(SQLiteConnection m_dbConnection, string namedb, string tabledb)
        {
            List<string> columns = new List<string>();
           
            string sql = "PRAGMA table_info(\""+ tabledb + "\")";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            using (SQLiteDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                  //  var fff = reader.GetColumnSchema();
                   // Console.WriteLine(reader.GetName(0)+" "+ reader.GetName(1) + " " +reader.GetName(2) );
                    columns.Add(reader[reader.GetName(1)].ToString());
                }
            }

            return columns;
        }
        private static void AddToTableDB(SQLiteConnection m_dbConnection, string nameDB, string tableName, string[] columns, string gorparse)
        {
            string temp = "1;МАХ023259ТП;Есть;15.04.2024;для геологического изучения недр, включающего поиски и оценку месторождений полезных ископаемых;Другое;Чебарда-1;Республика Дагестан;\"Чебарда-1 (1)  Тип пространственного объекта - Полигон  Система координат - ГСК-2011  № точки  Ш(гр,мин,сек)       Д(гр,мин,сек)  1        42°25'13.235\"\"N      47°09'42.698\"\"Е        2        42°25'11.655\"\"N      47°09'44.408\"\"Е        3        42°25'9.525\"\"N       47°09'45.648\"\"Е        4        42°25'8.295\"\"N       47°09'45.988\"\"Е        5        42°25'6.625\"\"N       47°09'46.238\"\"Е        6        42°25'6.065\"\"N       47°09'46.618\"\"Е        7        42°25'5.595\"\"N       47°09'47.238\"\"Е        8        42°25'4.795\"\"N       47°09'44.248\"\"Е        9        42°25'6.505\"\"N       47°09'44.168\"\"Е        10       42°25'8.145\"\"N       47°09'43.928\"\"Е        11       42°25'11.815\"\"N      47°09'43.138\"\"Е        Верхняя граница - нижняя граница почвенного слоя, а при его отсутствии – граница земной поверхности и дна водоемов и водотоков  Нижняя граница - 10м   Чебарда-1(2)  Тип пространственного объекта - Полигон  Система координат - ГСК-2011  № точки  Ш(гр,мин,сек)       Д(гр,мин,сек)  1        42°25'4.315\"\"N       47°09'48.038\"\"Е        2        42°25'2.075\"\"N       47°09'49.728\"\"Е        3        42°24'59.225\"\"N      47°09'45.128\"\"Е        4        42°25'1.285\"\"N       47°09'44.818\"\"Е        5        42°25'2.845\"\"N       47°09'44.558\"\"Е        Верхняя граница - нижняя граница почвенного слоя, а при его отсутствии – граница земной поверхности и дна водоемов и водотоков  Нижняя граница - 10м  \";Участок недр местного значения;Индивидуальный предприниматель АЛИЕВ ОСМАН ЮСУПОВИЧ;Министерство природных ресурсов и экологии Республики Дагестан;Протокол комиссии № 50 от 04.04.2024;;;;;;07.04.2025;;https://rfgf.ru/ReestrLicPage/440591";



            string[] goparses;

            StringReader reader = new StringReader(gorparse);
            using (TextFieldParser parser = new TextFieldParser(reader))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(";");
                goparses = parser.ReadFields();

            }
            

            string sql = "Insert into "+tableName+" (";

            for (int i = 0; i < columns.Length; i++)
            {
                if (i != columns.Length - 1) sql += columns[i] + ", ";
                else sql += columns[i];
            }
            sql += ", AddDate, SK) values (";


            for (int i = 0; i < goparses.Length; i++)
            {
                if (i == 8)
                {
                   
                    sql += "'" + MeanInTable(goparses[i], nameDB, "MEAN") + "', ";

                }
                else if (i != goparses.Length - 1) sql += "'" + goparses[i].Replace("\'", "mut") + "'" + ", ";
                
                else sql += "'" + goparses[i] + "'";
            }
            sql += ", '"+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") +"', '"+ SkInTable(goparses[8], nameDB, "MEAN") + "')";
            
            
           
            // sql = "Insert into Parse (№_п_п, Государственный_регистрационный_номер, Наличие_полного_электронного_образа, Дата_присвоения_государственного_регистрационного_номера_лицензии, Целевое_назначение_лицензии, Вид_полезного_ископаемого, Наименование_участка_недр__предоставленного_в_пользование_по_лицензии__кадастровый_номер_месторождения_или_проявления_полезных_ископаемых_в_ГКМ, Наименование_субъекта_Российской_Федерации_или_иной_территории__на_которой_расположен_участок_недр, Географические_координаты_угловых_точек_участка_недр__верхняя_и_нижняя_границы_участка_недр, Статус_участка_недр, Сведения_о_пользователе_недр, Наименование_органа__выдавшего_лицензию, Реквизиты_документа__на_основании_которого_выдана_лицензия_на_пользование_недрами, Сведения_о_внесении_изменений_и_дополнений_в_лицензию_на_пользование_недрами__сведения_о_наличии_их_электронных_образов, Сведения_о_переоформлении_лицензии_на_пользование_недрами, Реквизиты_приказа_о_прекращении_права_пользования_недрами__приостановлении_или_ограничении_права_пользования_недрами, Дата_прекращения_права_пользования_недрами, Срок_и_условия_приостановления_или_ограничения_права_пользования_недрами, Дата_окончания_срока_действия_лицензии, Сведения_о_реестровых_записях_в_отношении_ранее_выданных_лицензий_на_пользование_соответствующим_участком_недр, Ссылка_на_карточку_лицензии) values ('16', '13', '14', '5', '2', '4', '1', '10', '11', '6', '7', '15', '7', '6', '1', '10', '8', '19', '19', '9', '18')";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            
            command.ExecuteNonQuery();

        }
        private static string SkInTable(string mean, string nameDB, string tableName)
        {

            Regex regex = new Regex(@"(?:ГСК-2011|СК-42|Пулково-42|WGS-84)");
            MatchCollection matches = regex.Matches(mean);
            string stroka = "";
            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    stroka = match.Value;
                    break;
                }

            }
            else
            {

               // Console.WriteLine("Совпадений не найдено");
                stroka = "-1";
            }

            return stroka;
        }
        private static string MeanInTable(string mean, string nameDB, string tableName)
        {


           // Regex regex = new Regex(@"\d{1,3}°\d{1,2}'\d{1,2}\.\d{0,3}""[NSEWЕ]");
            Regex regex = new Regex(@"\d{1,3}°\d{1,2}'\d{1,2}(?:\.\d{1,3})?""[NSEWЕ]");
            MatchCollection matches = regex.Matches(mean);
            string stroka = "";

            bigCount++;

            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    //Console.WriteLine(match.Value);
                    string[] massTemp = match.Value.Split('°');

                    Int32 a = Convert.ToInt32( massTemp[0]);
                    massTemp = massTemp[1].Split('\'');
                    Int32 b = Convert.ToInt32(massTemp[0]);
                    massTemp = massTemp[1].Split('\"');
                    float c = (float)Convert.ToDouble(massTemp[0].Replace('.',','));

                    stroka+= AddMeanToTablenMEAN(a,b,c,nameDB,tableName)+ massTemp[1]+' ';
                }

            }
            else
            {
               
                Console.WriteLine(bigCount+" x "+ stopwatch.ElapsedMilliseconds/1000);
                stroka = "-1";
            }

            return stroka;
        } 
        private static string AddMeanToTablenMEAN(Int32 a, Int32 b, float c, string nameDB, string tableName)
        {

            string num = null;
            string connectionString = "Data Source=" + nameDB + ".sqlite;Version=3;";
            SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString);
            m_dbConnection.Open();


            string  sql = "INSERT INTO MEAN (Grad, Minute, Second)\r\nSELECT '"+a+"', '"+b+"', '"+c+"'\r\n" +
                "WHERE NOT EXISTS (SELECT id FROM MEAN WHERE Grad = '"+a+"' AND Minute = '"+b+"' AND Second = '"+c+"');\r\n\r\n" +
                "SELECT id\r\nFROM MEAN\r\nWHERE Grad = '"+a+"' AND Minute = '"+b+"' AND Second = '"+c+"';\r\n";

          //  sql = "SELECT id FROM MEAN WHERE Grad = '0' AND Minute = '0' AND Second = '0'";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);

            using (SQLiteDataReader reader = command.ExecuteReader())
            {
                if (reader.HasRows) // если есть данные
                {
                   
                    while (reader.Read())   // построчно считываем данные
                    {
                        num = reader.GetValue(0).ToString();
                       
                    }
                }
            }

            m_dbConnection.Close();

            return num;
        }
        private static void CreateTableDB(SQLiteConnection m_dbConnection, string nameDB, string tableName, string[] columns, string[] SK) {
           

            // varchar will likely be handled internally as TEXT
            // the (20) will be ignored
            // see https://www.sqlite.org/datatype3.html#affinity_name_examples


            string sql = "CREATE TABLE IF NOT EXISTS " + tableName + " (id INTEGER PRIMARY KEY AUTOINCREMENT,";     //name varchar(20), score int)";
            for(int i =0; i < columns.Length;i++)
            {
                if(i != columns.Length-1) sql += columns[i] + ", ";
                else sql += columns[i];
            }
            sql += ", AddDate DATE, SK TEXT)";

            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.ExecuteNonQuery();


            //Создание таблиц с названиями систем координат, можно в отдельный метод

            sql = "CREATE TABLE IF NOT EXISTS " + "CK" + " (id INTEGER PRIMARY KEY AUTOINCREMENT, "+
                "CK)";    
            command = new SQLiteCommand(sql, m_dbConnection);
            command.ExecuteNonQuery();


            foreach(string data in SK)
            {
                sql = "Insert into " + "CK" + " (" + "CK" + ") values (";
                sql += "'" + data + "'";
                sql += ")";
                command = new SQLiteCommand(sql, m_dbConnection);
                command.ExecuteNonQuery();
            }
            //Создание таблиц с числами координат, можно в отдельный метод

            sql = "CREATE TABLE IF NOT EXISTS " + "MEAN" + " (id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "Grad INTEGER, Minute INTEGER, Second TEXT)";
            command = new SQLiteCommand(sql, m_dbConnection);
            command.ExecuteNonQuery();


           
                sql = "Insert into " + "MEAN" + " (" + "Grad, Minute, Second" + ") values (";
                sql += "'" + 0 + "', "+ "'" + 0 + "', "+ "'" + 0 + "'";
                sql += ")";
                command = new SQLiteCommand(sql, m_dbConnection);
                command.ExecuteNonQuery();
           


          
        }

        private static void CreateDB(string name)
        {
            SQLiteConnection.CreateFile(name + ".sqlite");
        }

        private static string Translite(string s)
        {
            StringBuilder ret = new StringBuilder();
            string[] rus = { "А", "Б", "В", "Г", "Д", "Е", "Ё", "Ж", "З", "И", "Й",
          "К", "Л", "М", "Н", "О", "П", "Р", "С", "Т", "У", "Ф", "Х", "Ц",
          "Ч", "Ш", "Щ", "Ъ", "Ы", "Ь", "Э", "Ю", "Я" };
            string[] eng = { "A", "B", "V", "G", "D", "E", "E", "ZH", "Z", "I", "Y",
          "K", "L", "M", "N", "O", "P", "R", "S", "T", "U", "F", "KH", "TS",
          "CH", "SH", "SHCH", null, "Y", null, "E", "YU", "YA" };

            for (int j = 0; j < s.Length; j++)
                for (int i = 0; i < rus.Length; i++)
                    if (s.Substring(j, 1) == rus[i]) ret.Append(eng[i]);
            return ret.ToString();
        }

    }
}
