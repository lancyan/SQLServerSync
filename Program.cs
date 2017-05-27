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

namespace SQLServerSync
{
    class Program
    {

        static string sourceConnectString = System.Configuration.ConfigurationManager.ConnectionStrings["sourceDB"].ConnectionString;
        static string targetConnectString = System.Configuration.ConfigurationManager.ConnectionStrings["targetDB"].ConnectionString;


        static void Main(string[] args)
        {
            if (GetRunningInstance() != null)
            {
                Console.WriteLine("已经运行了一个实例了。");
                return;
            }

            List<SyncMap> synclist = new List<SyncMap>();


            synclist.Add(new SyncMap() { SourceTableName = "sync_test", TargetTableName = "sync_test", IsCheckTableSchema = false, IsDeleteTargetRow = true });
          


            SyncService ss = new SyncService(synclist, sourceConnectString, targetConnectString);


            Console.WriteLine(" 按回车键结束程序");
            Console.WriteLine(" 等待程序的执行．．．．．．");
            Stopwatch oTime = new Stopwatch();


            while (true)
            {
                Console.WriteLine(DateTime.Now.ToLongTimeString() + "  数据同步程序执行开始．．．．．．");
                oTime.Start();
                ss.DoSyncTask();
                oTime.Stop();
                Console.WriteLine(DateTime.Now.ToLongTimeString() + "  数据同步程序执行结束．．．．．．");
                Console.WriteLine("总共耗时" + oTime.Elapsed.TotalMilliseconds + "ms");
                Console.WriteLine();
                oTime.Reset();
                Thread.Sleep(10 * 1000);
            }
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
