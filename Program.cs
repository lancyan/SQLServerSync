using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Timers;
using System.Data;

using System.Windows.Forms;
using System.Diagnostics;
using System.Collections;
using System.Threading;
using System.ComponentModel;
using System.Reflection;
using System.Xml;
using System.IO;

namespace ZD.SyncDB
{
    class Program
    {

        static string sourceConnectString = System.Configuration.ConfigurationManager.ConnectionStrings["sourceDB"].ConnectionString;
        static string targetConnectString = System.Configuration.ConfigurationManager.ConnectionStrings["targetDB"].ConnectionString;
        static string timeFormat = System.Configuration.ConfigurationManager.AppSettings["timeFormat"].ToString();
        static int pageSize = Int32.Parse(System.Configuration.ConfigurationManager.AppSettings["pageSize"].ToString());
        static int sleepTime = Int32.Parse(System.Configuration.ConfigurationManager.AppSettings["sleepTime"].ToString());
        static int threadCount = Int32.Parse(System.Configuration.ConfigurationManager.AppSettings["threadCount"].ToString());
        static LogWriter log = new LogWriter("sync_log.txt");

        static void Main(string[] args)
        {
            if (threadCount > 0)
            {
                if (GetRunningInstance() != null)
                {
                    Console.WriteLine("已经运行了一个实例了。");
                    return;
                }
            }
          

            List<SyncMap> synclist = new List<SyncMap>();

            XmlDocument doc = new XmlDocument();
            if (File.Exists("db.xml"))
            {
                doc.Load("db.xml");
            }
            else
            {
                WriteLog("同步文件不存在");
                return;
            }
            XmlNode root = doc.SelectSingleNode("./sync");
            XmlNode ele = root.SelectSingleNode("./db");

            XmlNodeList nodelist = ele.SelectNodes("./table");


            SyncMap parentSM = ReadRootNode(ele);

            for (int x = 0, count = nodelist.Count; x < count; x++)
            {
                var node = nodelist[x];
                SyncMap childSM = ReadRootNode(node);
                IntersectionMap(parentSM, childSM);
                childSM.SourceTableName = node.Attributes["SourceTableName"].Value;
                childSM.TargetTableName = node.Attributes["TargetTableName"].Value;
                synclist.Add(childSM);
            }

          
            Console.WriteLine(" 程序开始执行......");
            Stopwatch oTime = new Stopwatch();


            while (true)
            {
                SyncService ss = null;
                try
                {
                    ss = new SyncService(log, pageSize, timeFormat, synclist, sourceConnectString, targetConnectString);
                }
                catch (Exception e1)
                {
                    WriteLog(e1.Message);
                }
                if (ss == null)
                {
                    WriteLog("数据库连接失败, 等待10秒......");
                    Thread.Sleep(10 * 1000);
                    continue;
                }
                WriteLog(DateTime.Now.ToLongTimeString() + "  数据同步程序执行开始．．．．．．");
                //<===========
                oTime.Start(); 
                ss.DoSyncTask();
                oTime.Stop();
                WriteLog(DateTime.Now.ToLongTimeString() + "  数据同步程序执行结束．．．．．．总共耗时" + oTime.Elapsed.TotalMilliseconds + "ms");
                Console.WriteLine();
                Console.WriteLine();
                oTime.Reset();
                //===========>
                ss.Close();
                Thread.Sleep(sleepTime);
            }
        }

        #region copy node

        private static K TransReflection<T, K>(T inObj)
        {
            K tOut = Activator.CreateInstance<K>();
            var inType = inObj.GetType();
            foreach (var itemOut in tOut.GetType().GetProperties())
            {
                var itemIn = inType.GetProperty(itemOut.Name); ;
                if (itemIn != null)
                {
                    itemOut.SetValue(tOut, itemIn.GetValue(inObj));
                }
            }
            return tOut;
        }

        private static SyncMap ReadRootNode(XmlNode node)
        {
            SyncMap sm = new SyncMap();

            sm.IsCheckTableSchema = IsDefined(node, "IsCheckTableSchema");

            sm.IsAddSync = IsDefined(node, "IsAddSync"); 

            sm.IsDeleteTargetRow = IsDefined(node, "IsDeleteTargetRow"); 

            sm.Direction = GetDefined(node, "Direction");

            sm.SubTimeSpan = GetDefined(node, "SubTimeSpan");

            return sm;
        }

        private static int GetDefined(XmlNode node, string key)
        {
            int result = 0;

            if (!string.IsNullOrEmpty(key))
            {
                XmlAttribute attr = node.Attributes[key];
                if (attr != null)
                {
                    string val = attr.Value;
                    if (!string.IsNullOrWhiteSpace(val))
                    {
                        Int32.TryParse(val, out result);
                    }
                }
            }
            return result;
        }

        private static bool IsDefined(XmlNode node, string key)
        {
            bool result = false;

            if (!string.IsNullOrEmpty(key))
            {
                XmlAttribute attr = node.Attributes[key];
                if (attr != null)
                {
                    string val = attr.Value;
                    if (!string.IsNullOrWhiteSpace(val))
                    {
                        if (val.Equals("1") || val.Equals("true", StringComparison.OrdinalIgnoreCase))
                        {
                            result = true;
                        }
                    }
                }
            }
            return result;
        }

        private static T IntersectionMap<T>(T p, T c)
        {
            var type = typeof(T);
            var pis = type.GetProperties();
            foreach (var pi in pis)
            {
                var pName = type.GetProperty(pi.Name);
                if (pName != null)
                {
                    object pv = pi.GetValue(p);
                    object cv = pi.GetValue(c);
                    if (!pv.Equals(cv) && IsDefault(pi.PropertyType, cv))
                    {
                        pi.SetValue(c, pv);
                    }
                }
            }
            return c;
        }

        static DateTime defaultDate = new DateTime(1900, 1, 1);

        private static bool IsDefault(Type type, object v)
        {
            bool isDefault = false;

            if (type.IsValueType)
            {
                if (type.Name == "Nullable`1")
                {
                    if (v == null)
                    {
                        isDefault = true;
                    }
                }
                else if (type.Name == "DateTime")
                {
                    if (DateTime.MinValue.Equals(v) || defaultDate.Equals(v))
                    {
                        isDefault = true;
                    }
                }
                else
                {
                    var obj = Activator.CreateInstance(type);
                    if (obj.Equals(v))
                    {
                        isDefault = true;
                    }
                }
            }
            else
            {
                if (v == null || string.IsNullOrEmpty(v.ToString()))
                {
                    isDefault = true;
                }
            }
            return isDefault;
        }

        #endregion


        private static void WriteLog(string msg)
        {
            Console.WriteLine(msg);
            log.WriteLine(msg);
        }

        public static Process GetRunningInstance()
        {
            Process current = Process.GetCurrentProcess();
            Process[] processes = Process.GetProcessesByName(current.ProcessName);
            foreach (Process process in processes)
            {
                if (process.Id != current.Id)
                {
                    if (Assembly.GetExecutingAssembly().Location.Replace("/", "\\") == current.MainModule.FileName)
                    {
                        return process;
                    }
                }
            }
            return null;
        }





    }
}
