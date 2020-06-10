using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace Earthquakes
{
    class Program
    {
        public struct ConfigFiles
        {
            public string mysqlCredentialsSelect_JsonFile { get; set; }
            public string mysqlCredentialsInsert_JsonFile { get; set; }
            public string userSites_JsonFile { get; set; }
        }

        public struct Coordinates
        {
            public double latitude { get; set; }
            public double longitude { get; set; }
        }

        public struct UserSite
        {
            public string siteName { get; set; }
            public Coordinates coordinates { get; set; }
        }

        public class Metadata
        {
            public long generated { get; set; }
            public string url { get; set; }
            public string title { get; set; }
            public int status { get; set; }
            public string api { get; set; }
            public int count { get; set; }
        }

        public class Properties
        {
            public double mag { get; set; }
            public string place { get; set; }
            public long time { get; set; }
            public long updated { get; set; }
            public int tz { get; set; }
            public string url { get; set; }
            public string detail { get; set; }
            public string felt { get; set; }
            public string cdi { get; set; }
            public string mmi { get; set; }
            public string alert { get; set; }
            public string status { get; set; }
            public int tsunami { get; set; }
            public int sig { get; set; }
            public string net { get; set; }
            public string code { get; set; }
            public string ids { get; set; }
            public string sources { get; set; }
            public string types { get; set; }
            public int nst { get; set; }
            public double dmin { get; set; }
            public double rms { get; set; }
            public double gap { get; set; }
            public string magType { get; set; }
            public string type { get; set; }
            public string title { get; set; }
        }

        public class Geometry
        {
            public string type { get; set; }
            public List<double> coordinates { get; set; }
        }

        public class FeaturesItem
        {
            public string type { get; set; }
            public Properties properties { get; set; }
            public Geometry geometry { get; set; }
            public string id { get; set; }
        }

        public class EarthquakesResult
        {
            public string type { get; set; }
            public Metadata metadata { get; set; }
            public List<FeaturesItem> features { get; set; }
            public List<double> bbox { get; set; }
        }

        public static double GetDistance(double latitude1, double longitude1, double latitude2, double longitude2)
        {
            var d1 = latitude1 * (Math.PI / 180.0);
            var num1 = longitude1 * (Math.PI / 180.0);
            var d2 = latitude2 * (Math.PI / 180.0);
            var num2 = longitude2 * (Math.PI / 180.0) - num1;
            var d3 = Math.Pow(Math.Sin((d2 - d1) / 2.0), 2.0) + Math.Cos(d1) * Math.Cos(d2) * Math.Pow(Math.Sin(num2 / 2.0), 2.0);

            return 3961 * (2.0 * Math.Atan2(Math.Sqrt(d3), Math.Sqrt(1.0 - d3)));
        }

        public static string Get_Earthquakes_JSON()
        {
            string earthquakeJSON = string.Empty;
            string url = @"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson";
            // string url = @"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson";
            // string url = @"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson";

            try
            {

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (Stream stream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {
                    earthquakeJSON = reader.ReadToEnd();
                }
            }
            catch (Exception e)
            {

            }
            return earthquakeJSON;
        }

        public class Event_Notification
        {
            public string eventNotification_Agency { get; set; }
            public string eventNotification_Title { get; set; }
            public string eventNotification_URL { get; set; }
            public long eventNotification_DatetimeEpoch { get; set; }
            public string eventNotification_Category { get; set; }
            public string eventNotification_Type { get; set; }
            public string eventNotification_UniqueID { get; set; }
            public double eventNotification_Latitude { get; set; }
            public double eventNotification_Longitude { get; set; }
        }

        public static void Add_Event_Notification(ConfigFiles jsonConfigPaths, Event_Notification eventNotification)
        {

            MySql.Data.MySqlClient.MySqlConnection conn;

            conn = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlConnectionStringBuilder conn_string_builder = new MySqlConnectionStringBuilder();
            string json = System.IO.File.ReadAllText(jsonConfigPaths.mysqlCredentialsInsert_JsonFile);

            conn = new MySqlConnection(conn_string_builder.ToString());
            try
            {
                conn.Open();
            }
            catch (Exception erro)
            {
                Console.WriteLine(erro);
            }

            try
            {
                MySqlCommand cmd = conn.CreateCommand();
                cmd.Connection = conn;

                cmd.CommandText = "INSERT INTO `geo_data`.`geo_events` (`geo_event_agency`,`geo_event_title`,`geo_event_url`,`geo_event_starttime`,`geo_event_category`,`geo_event_type`,`geo_event_ident`,`geo_event_location_latitude`,`geo_event_location_longitude`,`geo_event_notify`) VALUES (@event_notification_agency,@event_notification_title,@event_notification_url,FROM_UNIXTIME(@event_notification_datetime),@event_notification_category,@event_notification_type,@event_notification_ident,@event_notification_latitude,@event_notification_longitude,1);";
                cmd.Parameters.AddWithValue("@event_notification_agency", eventNotification.eventNotification_Agency);
                cmd.Parameters.AddWithValue("@event_notification_title", eventNotification.eventNotification_Title);
                cmd.Parameters.AddWithValue("@event_notification_url", eventNotification.eventNotification_URL);
                cmd.Parameters.AddWithValue("@event_notification_datetime", eventNotification.eventNotification_DatetimeEpoch);
                cmd.Parameters.AddWithValue("@event_notification_category", eventNotification.eventNotification_Category);
                cmd.Parameters.AddWithValue("@event_notification_type", eventNotification.eventNotification_Type);
                cmd.Parameters.AddWithValue("@event_notification_ident", eventNotification.eventNotification_UniqueID);
                cmd.Parameters.AddWithValue("@event_notification_latitude", eventNotification.eventNotification_Latitude);
                cmd.Parameters.AddWithValue("@event_notification_longitude", eventNotification.eventNotification_Longitude);

                cmd.ExecuteNonQuery();

                cmd.Dispose();
            }
            catch (MySqlException ex)
            {
                int errorcode = ex.Number;
                if (errorcode != 1062)
                {
                    Console.WriteLine("Notification Error:\t" + ex.Message);
                    Console.Read();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            conn.Close();

        }

        public static void Process_Applicable_Notification(ConfigFiles jsonConfigPaths, FeaturesItem earthquake)
        {
            List<Event_Notification> eventNotificationList = new List<Event_Notification>();
            List<UserSite> userSiteList = new List<UserSite>();

            string json = System.IO.File.ReadAllText(jsonConfigPaths.userSites_JsonFile);
            userSiteList = JsonConvert.DeserializeObject<List<UserSite>>(json);

            foreach (UserSite user_site in userSiteList)
            {
                double earthquake_latitude = earthquake.geometry.coordinates[1];
                double earthquake_longitude = earthquake.geometry.coordinates[0];

                double hq_distance = GetDistance(earthquake_latitude, earthquake_longitude, user_site.coordinates.latitude, user_site.coordinates.longitude);

                double magnitude = earthquake.properties.mag;

                if (((hq_distance < 50) || (earthquake.properties.tsunami.Equals(1)) || (magnitude >= 2 && hq_distance < 100) || (magnitude >= 3 && hq_distance < 250) || (magnitude >= 4 && hq_distance < 500) || (magnitude >= 5 && hq_distance < 1500) || (magnitude >= 6)))
                {
                    if (earthquake.properties.tsunami.Equals(1))
                    {
                        string notify_place = earthquake.properties.place + " Tsunami Evaluation Available";
                        earthquake.properties.place = notify_place;
                    }

                    Event_Notification eventNotification = new Event_Notification();
                    eventNotification.eventNotification_Agency = "48941";
                    eventNotification.eventNotification_Title = earthquake.properties.title;
                    eventNotification.eventNotification_URL = earthquake.properties.url;
                    eventNotification.eventNotification_DatetimeEpoch = (earthquake.properties.time / 1000);
                    eventNotification.eventNotification_Category = "Earthquake";
                    eventNotification.eventNotification_Type = "Earthquake";
                    eventNotification.eventNotification_UniqueID = earthquake.id;
                    eventNotification.eventNotification_Latitude = earthquake.geometry.coordinates[1];
                    eventNotification.eventNotification_Longitude = earthquake.geometry.coordinates[0];
                    Add_Event_Notification(jsonConfigPaths, eventNotification);
                }

            }


        }

        public static void Add_New_Earthquakes_To_Database(ConfigFiles jsonConfigPaths, List<FeaturesItem> newEarthquakesList)
        {
            MySql.Data.MySqlClient.MySqlConnection conn;

            conn = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlConnectionStringBuilder conn_string_builder = new MySqlConnectionStringBuilder();
            string json = System.IO.File.ReadAllText(jsonConfigPaths.mysqlCredentialsInsert_JsonFile);
            conn_string_builder = JsonConvert.DeserializeObject<MySqlConnectionStringBuilder>(json);

            conn = new MySqlConnection(conn_string_builder.ToString());
            try
            {
                conn.Open();
            }
            catch (Exception erro)
            {
                Console.WriteLine(erro);
            }

            foreach (FeaturesItem earthquake in newEarthquakesList)
            {
                try
                {
                    MySqlCommand cmd = conn.CreateCommand();
                    cmd.Connection = conn;

                    Console.WriteLine(earthquake.id + "\t" + earthquake.properties.title);
                    cmd.CommandText = "INSERT INTO `geo_data`.`geo_quakes` (`geo_quake_id`, `geo_quake_title`, `geo_quake_place`, `geo_quake_epoch`, `geo_quake_latitude`, `geo_quake_longitude`, `geo_quake_magntiude`, `geo_quake_depth`, `geo_quake_tsunami_alert`) VALUES (@quake_id, @title, @place, @epoch, @latitude, @longitude, @magnitude, @depth, @tsunami);";

                    cmd.Parameters.AddWithValue("@quake_id", earthquake.id);
                    cmd.Parameters.AddWithValue("@title", earthquake.properties.title);
                    cmd.Parameters.AddWithValue("@place", earthquake.properties.place);
                    cmd.Parameters.AddWithValue("@epoch", earthquake.properties.time / 1000);
                    cmd.Parameters.AddWithValue("@latitude", earthquake.geometry.coordinates[1]);
                    cmd.Parameters.AddWithValue("@longitude", earthquake.geometry.coordinates[0]);
                    cmd.Parameters.AddWithValue("@magnitude", earthquake.properties.mag);
                    cmd.Parameters.AddWithValue("@depth", earthquake.geometry.coordinates[2]);
                    cmd.Parameters.AddWithValue("@tsunami", earthquake.properties.tsunami);

                    int insert_status = cmd.ExecuteNonQuery();

                    if (insert_status == 1)
                    {
                        Process_Applicable_Notification(jsonConfigPaths, earthquake);
                    }
                    cmd.Dispose();
                }
                catch (MySqlException sql_exception)
                {
                    int errorCode = sql_exception.ErrorCode;
                    if (errorCode != 1062)
                    {
                        Console.WriteLine("Quake Add Error:\t" + sql_exception.Message);
                        Console.Read();
                    }
                }
            }


            conn.Close();

        }

        public static List<string> Get_Recent_Existing_Earthquake_IDs(ConfigFiles jsonConfigPaths)
        {
            List<string> existingEarthquakeList = new List<string>();

            MySql.Data.MySqlClient.MySqlConnection conn;
            conn = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlConnectionStringBuilder conn_string_builder = new MySqlConnectionStringBuilder();

            string json = System.IO.File.ReadAllText(jsonConfigPaths.mysqlCredentialsSelect_JsonFile);
            conn_string_builder = JsonConvert.DeserializeObject<MySqlConnectionStringBuilder>(json);

            conn = new MySqlConnection(conn_string_builder.ToString());

            try
            {
                conn.Open();
            }
            catch (Exception erro)
            {
                Console.WriteLine(erro);
            }

            MySqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = "SELECT `geo_quake_id`  FROM `geo_data`.`geo_quakes` WHERE FROM_UNIXTIME(`geo_quake_epoch`) > DATE_SUB(NOW(),INTERVAL 2 DAY);";

            try
            {
                MySqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    string geo_quake_id = reader[0].ToString();
                    existingEarthquakeList.Add(geo_quake_id);
                }

                conn.Close();
            }
            catch (Exception erro)
            {
                Console.WriteLine(erro);
            }

            return existingEarthquakeList;
        }

        public static Boolean Is_New_Earthquake(string earthquakeID, List<string> recentEarthquakesList)
        {
            Boolean newEarthquake = true;
            foreach (String recentEarthquakeId in recentEarthquakesList)
            {
                if (earthquakeID.Equals(recentEarthquakeId).Equals(true))
                {
                    newEarthquake = false;
                }
            }

            return newEarthquake;
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Earthquake Feed Reader");

            string configFilePaths = "filePaths.json";
            bool exists = File.Exists(configFilePaths);
            string json = null;

            try
            {
                json = System.IO.File.ReadAllText(configFilePaths, System.Text.Encoding.UTF8);
            }
            catch (Exception json_read)
            {
                Console.WriteLine(json_read.Message);
            }

            if (json != null) // Check That JSON String Read Above From File Contains Data
            {
                ConfigFiles jsonConfigPaths = new ConfigFiles();
                jsonConfigPaths = JsonConvert.DeserializeObject<ConfigFiles>(json);

                List<string> existingEarthquakesList = Get_Recent_Existing_Earthquake_IDs(jsonConfigPaths);

                string earthquakes_json = Get_Earthquakes_JSON();
                var settings = new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore
                };

                EarthquakesResult recent_earthquakes = JsonConvert.DeserializeObject<EarthquakesResult>(earthquakes_json, settings);

                List<FeaturesItem> newEarthquakesList = new List<FeaturesItem>();
                foreach (FeaturesItem earthquake in recent_earthquakes.features)
                {
                    Boolean newEarthquake = Is_New_Earthquake(earthquake.id, existingEarthquakesList);
                    if (newEarthquake == true)
                    {
                        newEarthquakesList.Add(earthquake);
                    }
                }
                Add_New_Earthquakes_To_Database(jsonConfigPaths, newEarthquakesList);
            }
        }
    }
}
