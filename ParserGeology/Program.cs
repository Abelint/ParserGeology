using System.Text;

using System.Data.SQLite;

using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;
using System.Diagnostics;

using System.Data;
using System.Xml.Linq;


namespace ParserGeology
{
    internal class Program
    {
        //668 x 23 операций х секунды без потоков
        static Stopwatch stopwatch;
        static int bigCount = 0;
        static SQLiteConnection m_dbConnection;
        static   string namedb = "Parser_new";
        static  string tableName = "Parse";
        static string newTableName = "formap";
        static List<Thread> threads = new List<Thread>();
        static List<string> gcndb;
        static List<string> massData = new List<string>();

        static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(50);

        static MeanTable[] means;
        static List<MeanTable> meanList = new List<MeanTable>();
        static void Main(string[] args)
        {
            Console.WriteLine("Введите действие\n1-создать исходную базу данных\n2-добавить столбец с координатами для сайта");
            int key = Convert.ToInt32( Console.ReadLine());

            switch (key)
            {
                case 2:
                    CreateCoordFromDB();
                    break;

                case 1:
                    CreateNewDB();
                    break;
            }

           
        }

        static void CreateCoordFromDB()
        {
            Console.WriteLine("Модернизируем БД");
            if(!File.Exists(namedb + ".sqlite"))
            {
                Console.WriteLine("БД отсутствует");
                return;
            }
            if (m_dbConnection == null)
            {
                string connectionString = "Data Source=" + namedb + ".sqlite;Version=3;";
                m_dbConnection = new SQLiteConnection(connectionString);
            }
            m_dbConnection.Open();

            // Проверяем наличие таблицы
            string checkTableQuery = $"SELECT name FROM sqlite_master WHERE type='table' AND name='{newTableName}'";
            using (var cmd = new SQLiteCommand(checkTableQuery, m_dbConnection))
            {
                var tableExists = cmd.ExecuteScalar() != null;

                if (!tableExists)
                {
                    // Создаем новую таблицу с нужной структурой
                    string createTableQuery = $@"
                    CREATE TABLE {newTableName} (
                    id INTEGER,
                    WGS84_grad TEXT,
                    formap TEXT
                )";
                    using (var createTableCmd = new SQLiteCommand(createTableQuery, m_dbConnection))
                    {
                        createTableCmd.ExecuteNonQuery();
                        Console.WriteLine($"Таблица '{newTableName}' создана.");
                    }
                }
                else
                {
                    Console.WriteLine($"Таблица '{newTableName}' уже существует.");
                }
            }


            string getDataQuery = $"SELECT id, WGS84_grad FROM {tableName}";
            using (var cmd = new SQLiteCommand(getDataQuery, m_dbConnection))
            {
                using (var reader = cmd.ExecuteReader())
                {
                    long count =0;
                    while (reader.Read())
                    {
                        // Получаем ID и значение WGS84_grad
                        int rowId = reader.GetInt32(0); // Предполагаем, что ID - это первый столбец
                        string data = reader["WGS84_grad"].ToString();

                        // Преобразуем координаты
                        var transformedData = TransformCoordinates(data);
                        count++;
                        // Обновляем значение в столбце formap
                        if (transformedData != null) InsertIntoNewTable(rowId, data, transformedData);
                    }
                }
            }
        }

        static List<string> TransformCoordinates(string data)
        {
            List<string> coordinates = new List<string>();
            string result = "";
            var mass = data.Split(' ');
            if (mass.Length < 2 ) return null;
            int num = -1;
            int lastnum = -1;
            bool except = false;
            bool addexcept= false;
            for (int i = 0; i < mass.Length; i++)
            {
                if (mass[i] == "Исключаемая")
                {
                    if(except) addexcept = true;
                    except = true;

                }
                var point = mass[i].Split(':');
                if (point.Length < 2 ) continue;

                num =int.Parse(point[0]);
                var tg = point[1].Split("g");
                var grad = tg[0];
                var tmin = tg[1].Split('m');
                var min = tmin[0];
                var tsec = tmin[1].Split('s');
                var sec = tsec[0];
                var dir = tsec[1];
                double mean = double.Parse(grad) + double.Parse(min)/60.0 + double.Parse(sec)/3600.0;
                if (lastnum < num)
                {
                    if (result != "") result += ", ";
                    result += "[";
                    result += mean.ToString().Replace(',', '.');
                    result += ",";
                }
                else if (lastnum == num)
                {

                    result += mean.ToString().Replace(',', '.');
                    result += "]";
                }
                else if (lastnum > num && (except||addexcept))
                {
                    result += "], [[";
                    result += mean.ToString().Replace(',', '.');
                    result += ",";
                    except = false;
                    addexcept = false;
                }
                else if (lastnum > num)
                {
                    result += "]";
                    coordinates.Add(result);
                    result = "";
                    result += "[";
                    result += mean.ToString().Replace(',', '.');
                    result += ",";
                }
                lastnum = num;
            }
            coordinates.Add(result);
            return coordinates;
        }
        // Метод для вставки данных в новую таблицу
        private static void InsertIntoNewTable(int id, string wgs84Grad, List<string> formap)
        {
            using (var transaction = m_dbConnection.BeginTransaction())
            {
                try
                {
                    string insertQuery = $"INSERT INTO {newTableName} (id, WGS84_grad, formap) VALUES (@id, @wgs84Grad, @formap)";
                    using (var cmd = new SQLiteCommand(insertQuery, m_dbConnection, transaction))
                    {
                        cmd.Parameters.Add(new SQLiteParameter("@id", DbType.Int32));
                        cmd.Parameters.Add(new SQLiteParameter("@wgs84Grad", DbType.String));
                        cmd.Parameters.Add(new SQLiteParameter("@formap", DbType.String));
                        cmd.Prepare(); // Подготавливаем команду

                        foreach (string s in formap)
                        {
                            cmd.Parameters["@id"].Value = id;
                            cmd.Parameters["@wgs84Grad"].Value = wgs84Grad;
                            cmd.Parameters["@formap"].Value = s;
                            cmd.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit(); // Фиксируем транзакцию
                }
                catch (Exception ex)
                {
                    transaction.Rollback(); // Откатываем транзакцию в случае ошибки
                    throw new Exception("Ошибка при вставке данных: " + ex.Message);
                }
            }
        }

        static void CreateNewDB()
        {
            string path = "opendata.csv";
            //string path = "opendata_short.csv";

            int lineCount = File.ReadLines(path).Count();

            string firstLine = File.ReadLines(path).ElementAtOrDefault(0);
            string[] col2 = firstLine.Split(';');
            string[] col = new string[col2.Length];
            for (int i = 0; i < col2.Length; i++)
            {
                col2[i] = col2[i].Trim().Replace(' ', '_').Replace('/', '_').Replace(',', '_');
                col[i] = col2[i] + " TEXT";
            }

            string[] CK = { "ГСК-2011", "СК-42", "WGS-84", "noName" };////////////////считать из бд или создать по этой строке, нужно добавить

            CreateDB(namedb);

            string connectionString = "Data Source=" + namedb + ".sqlite;Version=3;";
            if(m_dbConnection == null) m_dbConnection = new SQLiteConnection(connectionString);
            m_dbConnection.Open();

            CreateTableDB(m_dbConnection, namedb, tableName, col, CK);
            string sqlExpression = "SELECT * FROM MEAN";


            SQLiteCommand command = new SQLiteCommand(sqlExpression, m_dbConnection);
            using (SQLiteDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    MeanTable mean = new MeanTable();
                    mean.Id = (long)reader.GetValue(0);
                    mean.Grad = Convert.ToInt32(reader.GetValue(1));
                    mean.Minute = Convert.ToInt32(reader.GetValue(2));
                    mean.Second = (string)reader.GetValue(3);
                    meanList.Add(mean);
                }
            }
            means = meanList.ToArray();
            meanList.Clear();

            gcndb = GetColumnNameDB(m_dbConnection, namedb, tableName);


            Console.WriteLine("Создание базы участков");

            int count = 0;
            using (StreamReader reader = new StreamReader(path))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    count++;
                    if (count == 1) continue;
                    string[] goparses;

                    ParseStr(line);

                    //AddToTableDB(line);

                }
            }
            while (!CheckThreadsForEnd(threads)) ;
            stopwatch.Stop();
            long freq = Stopwatch.Frequency;
            double sec = (double)stopwatch.ElapsedTicks / freq; //переводим такты в секунды
            Console.WriteLine($"Частота таймера {freq} такт/с \r\n Время в тактах {stopwatch.ElapsedTicks} \r\n Время в секундах  {(double)stopwatch.ElapsedTicks / freq}");



            foreach (string gcn in gcndb)
            {
                Console.WriteLine(gcn);
            }
            Console.WriteLine("Идет обновление базы, ожидайте");
            AddMeanToTablenMEAN(meanList, "MEAN");
            m_dbConnection.Close();
            Console.WriteLine("Приложение можно закрыть");
        }


        static void ParseStr(object strokaForParse)
        {
           
            string[] goparses;

            StringReader reader = new StringReader((string)strokaForParse);
            using (TextFieldParser parser = new TextFieldParser(reader))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(";");
                goparses = parser.ReadFields();

            }

            ResultCallBackMethod(goparses);

        }

        static   private bool CheckThreadsForEnd(List<Thread> aThreads /* Список потоков */)
        {
            if (aThreads.Count == 0) { return true;/* 0 потоков*/}

            foreach (Thread CurThread in aThreads)
            {
                if (CurThread == null)
                {
                    return true;/*пусто*/
                }
                if (!(CurThread.Join(TimeSpan.Zero)))
                {
                    return false;// хоть один не завершился
                }

            }
            return true;// все завершились
        }
        /// <summary>
        /// То что вызывается при окончании потока
        /// </summary>
        /// <param name="goparses"></param>
        public static void ResultCallBackMethod(string[] goparses)
        {



            StringBuilder sqlSB = new StringBuilder();
           
            sqlSB.Append("Insert into " + tableName + " (");
           // string sql = "Insert into " + tableName + " (";

            for (int i = 1; i < gcndb.Count; i++)
            {
                //if (i != gcndb.Count - 1) sqlSB.Append(gcndb[i] + ", ");
                //else sqlSB.Append(gcndb[i]);
                switch (i.CompareTo(gcndb.Count - 1))
                {                   
                   
                    case 0:
                        sqlSB.Append(gcndb[i]);
                        break;
                   default:
                        sqlSB.Append(gcndb[i] + ", ");
                        break;

                }

            }





            sqlSB.Append(") values (");

            stopwatch = new Stopwatch();
            stopwatch.Start();
            long tiki = 0;

            for (int i = 0; i < goparses.Length; i++)
            {
                if (i == 8)
                {


                  // string look = MeanInTable(goparses[i], namedb, "MEAN");
                    string[] mass = goparses[i].Split(' ');
                    string append = "'";
                   
                    int count = 0;
                    bool except = false;
                    int lastPoint = 1;

                    string lastNum = "";
                    foreach (string s in mass)
                    {
                        count++;
                        int point = -1;

                       

                        if (int.TryParse(s, out point))
                        {
                            if (except && point == 1 && point != lastPoint)
                            {
                                append += "Основной ";
                                except = false;
                            }
                            lastNum = point + ":";                          
                            lastPoint = point;
                        }
                        else if (s == "Исключаемая")
                        {
                            except = true;
                            append += "Исключаемая ";
                        }
                        else
                        {
                            string pattern = "(\\d{1,3})°(\\d{1,2})'(\\d+(\\.\\d+)?)(['\"NSWE])"; // одна Е русская, вот так, East это теперь русское слово и ничего не спрашивайте
                            string temp = s.Trim();
                            Regex regex = new Regex(pattern);
                            MatchCollection matches = regex.Matches(temp);
                            char direction = ' ';


                            if (matches.Count > 0)
                            {
                                append += lastNum;
                                direction = temp.Last();
                                foreach (Match match in matches)
                                {
                                    var gr = match.Groups;
                                    append += $"{match.Groups[1].Value}g{match.Groups[2].Value}m{match.Groups[3].Value}s"+direction+ " ";
                                }
                            }
                            else  if (IsNotNumberAndNotProbel(s))
                            {
                                lastNum = "";
                            }

                        }
                    }

                    append += "', ";
                    sqlSB.Append(append);
                   
                }
                else if (i != goparses.Length - 1) sqlSB.Append("'" + goparses[i].Replace("\'", "mut") + "'" + ", ");

                else
                {
                    sqlSB.Append("'");
                    sqlSB.Append(goparses[i]);
                    sqlSB.Append("'");
                }
               
            }


            long ticks = stopwatch.Elapsed.Ticks;
          
            tiki = ticks;


            sqlSB.Append(", '");
            sqlSB.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            sqlSB.Append("', '" + SkInTable(goparses[8], namedb, "MEAN"));
            sqlSB.Append("')");


            // sql = "Insert into Parse (№_п_п, Государственный_регистрационный_номер, Наличие_полного_электронного_образа, Дата_присвоения_государственного_регистрационного_номера_лицензии, Целевое_назначение_лицензии, Вид_полезного_ископаемого, Наименование_участка_недр__предоставленного_в_пользование_по_лицензии__кадастровый_номер_месторождения_или_проявления_полезных_ископаемых_в_ГКМ, Наименование_субъекта_Российской_Федерации_или_иной_территории__на_которой_расположен_участок_недр, Географические_координаты_угловых_точек_участка_недр__верхняя_и_нижняя_границы_участка_недр, Статус_участка_недр, Сведения_о_пользователе_недр, Наименование_органа__выдавшего_лицензию, Реквизиты_документа__на_основании_которого_выдана_лицензия_на_пользование_недрами, Сведения_о_внесении_изменений_и_дополнений_в_лицензию_на_пользование_недрами__сведения_о_наличии_их_электронных_образов, Сведения_о_переоформлении_лицензии_на_пользование_недрами, Реквизиты_приказа_о_прекращении_права_пользования_недрами__приостановлении_или_ограничении_права_пользования_недрами, Дата_прекращения_права_пользования_недрами, Срок_и_условия_приостановления_или_ограничения_права_пользования_недрами, Дата_окончания_срока_действия_лицензии, Сведения_о_реестровых_записях_в_отношении_ранее_выданных_лицензий_на_пользование_соответствующим_участком_недр, Ссылка_на_карточку_лицензии) values ('16', '13', '14', '5', '2', '4', '1', '10', '11', '6', '7', '15', '7', '6', '1', '10', '8', '19', '19', '9', '18')";
            string te = sqlSB.ToString();
            massData.Add(te);
            SQLiteCommand command = new SQLiteCommand(te, m_dbConnection);

            command.ExecuteNonQuery();
            
           

        }

        public static bool IsNotNumberAndNotProbel(string input)
        {
            // Проверка на null и пробелы
            if (string.IsNullOrWhiteSpace(input))
            {
                return false; // Если строка пустая или состоит только из пробелов
            }

            // Проверка, является ли строка числом
            double number;
            bool isNumber = double.TryParse(input, out number);

            return !isNumber; // Возвращаем true, если строка не число
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
        private static void AddToTableDB(string gorparse)
        {
          
            // сюда потоки
            ///здесь создание потоков
            ResultCallbackDelegate resultCallbackDelegate = new ResultCallbackDelegate(ResultCallBackMethod);
            LineParser obj = new LineParser(gorparse, resultCallbackDelegate);
            //Creating the Thread using ThreadStart delegate
            Thread T1 = new Thread(new ThreadStart(obj.ParseStr));
            threads.Add(T1);
            
            T1.Start();
                
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

            string pattern = @"(\d+)\s+(\d+°\d+'\d+\.\d+\""[NS])\s+(\d+°\d+'\d+\.\d+\""[WEЕ])"; // одна Е русская, вот так, East это теперь русское слово и ничего не спрашивайте
          
            Regex regex = new Regex(pattern);
            MatchCollection matches = regex.Matches(mean);
            if (matches.Count == 0)
            {
                pattern = @"(\d+)\s+(\d+°\d+\'\d+\""[NS])\s+(\d+°\d+\'\d+\""[WEЕ])";
                regex = new Regex(pattern);
                matches = regex.Matches(mean);
            }
            foreach (Match match in matches)
            {
                Console.WriteLine($"{match.Groups[1].Value}\t{match.Groups[2].Value}\t{match.Groups[3].Value}");
            }

            // Regex regex = new Regex(@"\d{1,3}°\d{1,2}'\d{1,2}\.\d{0,3}""[NSEWЕ]");
            // string pattern = @"\d{1,3}°\d{1,2}'\d{1,2}(?:\.\d{1,3})?""[NSEWЕ]"; // без номеров но работает


            //string pattern = @"\d+\s\d+°\d+'\d+\.\d+""[NS]\s\d+°\d+'\d+\.\d+""[WE]";
            //Regex regex = new Regex(pattern);
            //MatchCollection matches = regex.Matches(mean);
            string stroka = "";


            if (matches.Count > 0)
            {
                foreach (Match match in matches)
                {
                    string input = match.Value;
                    string[] parts = input.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    string[] mas = new string[] { parts[0],parts[1],parts[2] };

                    string N =mas[0];
                   

                    for(int i = 1; i < mas.Length; i++)
                    {
                        string[] massTemp = mas[i].Split('°');
                        Int32 a = Convert.ToInt32(massTemp[0]);
                        massTemp = massTemp[1].Split('\'');
                        Int32 b = Convert.ToInt32(massTemp[0]);
                        massTemp = massTemp[1].Split('\"');
                        float c = (float)Convert.ToDouble(massTemp[0].Replace('.', ','));
                        // stroka += "[FF]" + massTemp[1] + ' ';
                        stroka += N + ":" + FindID(a, b, c) + massTemp[1] + ' ';
                    }

                  

                   
                }

            }
            else
            {
               
               // Console.WriteLine(bigCount+" x "+ stopwatch.ElapsedMilliseconds/1000);
                stroka = "-1";
            }

            return stroka;
        } 

        private static string FindID(Int32 a, Int32 b, float c)
        {
            foreach (MeanTable mean in means)
            {
                if (a == mean.Grad && b == mean.Minute && c.ToString() == mean.Second)
                {
                    return mean.Id.ToString();
                }
            }

            MeanTable tmp = new MeanTable
            {
                Id = -1,
                Grad = a,
                Minute = b,
                Second = c.ToString()
            };
            meanList.Add(tmp);

            long ID = means.Length + meanList.Count;
            return ID.ToString();
        }

        private static void AddMeanToTablenMEAN(List<MeanTable> means, string tableName)
        {
            string sql = "";
            int leng = 0;
            while (means.Count > 0)
            {
                leng++;              
                
                sql += "Insert into "+tableName+"(Grad, Minute, Second) VALUES('" + means[0].Grad +
                    "', '" + means[0].Minute + "', '" + means[0].Second + "'); ";
                means.RemoveAt(0);

                if(leng > 49) {
                    leng = 0;
                    SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                    command.ExecuteNonQuery();
                    sql = "";
                }


            }

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
            if(!File.Exists(name + ".sqlite")) SQLiteConnection.CreateFile(name + ".sqlite");
        }

     
    }
}
