using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using System.Web;
using System.Net;
using System.Diagnostics;
using System.Threading;

namespace ZD.SyncDB
{
    public class LogWriter
    {
        private string _fileName;

        private string _ext = ".txt";

        private long _fileSize;

        private static readonly object locker = new object();

        /// <summary>
        /// 获取或设置文件名称
        /// </summary>
        public string FileName
        {
            get
            {
                return _fileName;
            }
            set { _fileName = value; }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="byteCount">每次开辟位数大小，这个直接影响到记录文件的效率</param>
        /// <param name="fileName">文件全路径名</param>
        public LogWriter(string fileName = "", long fileSize = 1048576)
        {
            string s = Thread.GetDomain().FriendlyName;
            string rootPath = s.Contains("W3SVC") ? HttpContext.Current.Server.MapPath("~/") : System.AppDomain.CurrentDomain.BaseDirectory;
            if (!string.IsNullOrEmpty(fileName))
            {
                bool isNetAddress = false;
                if (Regex.IsMatch(fileName, @"\d+\.\d+\.\d+\.\d+"))
                {
                    isNetAddress = true;
                }
                if (!isNetAddress)
                {
                    Match mAddress = Regex.Match(fileName, @"^\\\\(.*?)\\");
                    if (mAddress.Success)
                    {
                        string machineName = mAddress.Result("$1");
                        IPAddress[] ipAddress = Dns.GetHostAddresses(machineName);
                        if (ipAddress.Length > 0)
                        {
                            isNetAddress = true;
                        }
                    }
                }
                //绝对路径或网络地址
                if (!isNetAddress && !Regex.IsMatch(fileName, "\\w:"))
                {
                    fileName = rootPath + fileName;
                }
                //if (!Regex.IsMatch(fileName, "\\.\\w+$"))
                //{
                //    fileName += "log" + _ext;
                //}
                if (fileName.IndexOf(_ext) == -1)
                {
                    int idx = fileName.LastIndexOf('.');
                    int length = fileName.Substring(idx).Length;
                    if (idx == -1 || length > 4)
                    {
                        fileName += _ext;
                    }
                    else if (length == 1)
                    {
                        fileName += "txt";
                    }
                }
            }
            else
            {
                fileName = rootPath + "log" + _ext;
            }
            _fileName = fileName;
            _fileSize = fileSize;
        }

        private static int CompareDinosByLength(FileInfo x, FileInfo y)
        {
            if (x == null)
            {
                if (y == null)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                if (y == null)
                {
                    return 1;
                }
                else
                {
                    int retval = x.Name.Length.CompareTo(y.Name.Length);
                    if (retval != 0)
                    {
                        return retval;
                    }
                    else
                    {
                        return x.Name.CompareTo(y.Name);
                    }
                }
            }
        }

        private string GetNewFileName()
        {
            FileInfo fi = new FileInfo(FileName);
            DirectoryInfo di = fi.Directory;
            string fileNameNoExt = Path.GetFileNameWithoutExtension(FileName);
            string newFileName = Regex.Replace(fileNameNoExt, "\\(\\d+\\)", "");
            string directName = di.FullName + "\\";
            FileInfo[] fis = di.GetFiles(newFileName + "*" + _ext);
            Array.Sort(fis, CompareDinosByLength);
            int idx = 1;
            int len = fis.Length;
            string fileFullName = "";
            if (len > 1)
            {
                for (int i = len - 1; i >= 0; i--)
                {
                    Match m = Regex.Match(fis[i].Name, "\\d+");
                    if (m.Success)
                    {
                        fileFullName = fis[i].FullName;
                        idx = Int32.Parse(m.Value);
                        break;
                    }
                }
                if (new FileInfo(fileFullName).Length >= _fileSize)
                {
                    idx++;
                    fileFullName = directName + newFileName + "(" + idx + ")" + _ext;
                }
            }
            else
            {
                fileFullName = directName + newFileName + "(" + idx + ")" + _ext;
            }
            return fileFullName;
        }


        /// <summary>
        /// 写入文件
        /// </summary>
        /// <param name="content"></param>
        public void Write(Exception ex, string newLine = "", Encoding encoding = null)
        {
            this.Write(ex.Message, newLine, encoding);
        }

        /// <summary>
        /// 写入文本
        /// </summary>
        /// <param name="content">文本内容</param>
        public void Write(string content, string newLine = "", Encoding encoding = null)
        {
            try
            {
                content += newLine;
                encoding = encoding ?? System.Text.Encoding.UTF8;
                lock (locker)
                {
                    FileInfo fi = new FileInfo(FileName);
                    if (!fi.Exists)
                    {
                        if (!fi.Directory.Exists)
                        {
                            fi.Directory.Create();
                        }
                    }
                    else
                    {
                        if (fi.Length >= _fileSize)
                        {
                            FileName = GetNewFileName();
                        }
                    }
                    using (StreamWriter sw = new StreamWriter(FileName, true, encoding))
                    {
                        sw.Write(content);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }


        /// <summary>
        /// 写入文件内容
        /// </summary>
        /// <param name="content"></param>
        public void WriteLine(string content, Encoding encoding = null)
        {
            this.Write(content, Environment.NewLine, encoding);
        }

        /// <summary>
        /// 写入文件内容
        /// </summary>
        /// <param name="content"></param>
        public void WriteLine(Exception ex, Encoding encoding = null)
        {
            this.Write(DateTime.Now.ToString() + "  " + ex.Message, Environment.NewLine, encoding);
            this.Write(DateTime.Now.ToString() + "  " + ex.StackTrace, Environment.NewLine, encoding);
        }




    }
}
