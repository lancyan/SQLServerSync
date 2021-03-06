using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ZD.SyncDB
{
    public class SyncService
    {
        LogWriter log = null;
        public SyncService(LogWriter lw, int pageSize, string timeFormat, List<SyncMap> smlist, string sourceConnectString, string targetConnectString)
        {
            this.synclist = smlist;

            if (smlist.Count > 0)
            {
                this.log = lw;
                this.pageSize = pageSize;
                this.pattern = timeFormat;
                this.sourceConnectString = sourceConnectString;
                this.targetConnectString = targetConnectString;

                this.SourceConnection = new SqlConnection(sourceConnectString);
                this.TargetConnection = new SqlConnection(targetConnectString);
                this.SourceConnection.Open();
                this.TargetConnection.Open();
            }
        }


        private List<SyncMap> synclist;

        private string sourceConnectString;

        private string targetConnectString;

        private SqlConnection _sourceConnection;

        private SqlConnection _targetConnection;

        private SqlConnection SourceConnection
        {
            get
            {
                if (_sourceConnection == null)
                {
                    _sourceConnection = new SqlConnection();
                }
                return _sourceConnection;
            }
            set
            {
                this._sourceConnection = value;
            }
        }

        private SqlConnection TargetConnection
        {
            get
            {
                if (_targetConnection == null)
                {
                    _targetConnection = new SqlConnection(targetConnectString);
                }
                return _targetConnection;
            }
            set
            {
                this._targetConnection = value;
            }
        }


        public void Close()
        {
            SourceConnection.Close();
            TargetConnection.Close();
        }
        private string pattern = "";
        //private int subTimeSpan = 0;

        Hashtable hashTable = new Hashtable(10240);

        int pageSize = 2000;
        const string countSql = "SELECT COUNT(0) FROM {0}";
        //sqlserver2000以上
        //const string selectSql = "SELECT TOP {0} * FROM [{1}] WHERE ID>ISNULL((SELECT MAX(ID) FROM(SELECT TOP {2} ID FROM [{1}] ORDER BY ID) AS TEMP),0)";
        //sqlserver2005以上
        const string selectColumns = "SELECT TOP 1 * FROM {0}";  //SELECT Name FROM SysColumns WHERE id=Object_Id('{0}') order by colstat desc
        const string selectSql = "SELECT * FROM (SELECT row_number() over(order by {0}) as RowIndex,* FROM {1}) as t where RowIndex between {2} and {3}";

        const string insertSql = "INSERT INTO {0}({1}) VALUES({2})";
        const string updateSql = "UPDATE {0} SET {1} WHERE {2}";
        const string deleteSql = "DELETE FROM {0} WHERE {1}";

        public void DoSyncTask()
        {
            //int pageIndex = 0;
            //string selectSql = @"select top {0} * from (select row_number() over(order by id) as rownumber,* from [{1}]) A where rownumber > {2}";
            if (synclist.Count > 0)
            {
                SqlCommand sourceCmd = new SqlCommand();
                SqlCommand targetCmd = new SqlCommand();

                SqlDataAdapter sourceAda = new SqlDataAdapter(sourceCmd);
                SqlDataAdapter targetAda = new SqlDataAdapter(targetCmd);


                foreach (var mapTable in synclist)
                {
                    try
                    {
                        //subTimeSpan = mapTable.SubTimeSpan;

                        if (mapTable.Direction == 0)
                        {
                            SyncTable(sourceAda, targetAda, mapTable, SourceConnection, TargetConnection);
                        }
                        else if (mapTable.Direction == 1)
                        {
                            SyncTable(targetAda, sourceAda, mapTable, TargetConnection, SourceConnection);
                        }
                    }
                    catch (Exception ex)
                    {
                        WriteLog(ex.Message);
                    }
                }
            }
          
        }

        private void WriteLog(string msg)
        {
            Console.WriteLine(msg);
            log.WriteLine(msg);
        }

        private string formatTableName(SyncMap mapTable, string tabName)
        {
            if (!string.IsNullOrEmpty(pattern))
            {
                if (Regex.IsMatch(tabName, pattern))
                {
                    string date = DateTime.Now.AddMonths(-mapTable.SubTimeSpan).ToString(pattern).TrimStart('{').TrimEnd('}');
                    tabName = Regex.Replace(tabName, Regex.Escape(pattern), date);
                }
            }
            return tabName;
        }


        private bool SyncColumns(SqlCommand sourceCmd, SqlCommand targetCmd, SyncMap mapTable,string sourceTableName,string targetTableName)
        {
            bool flag = false;
            //第一次查询记录
            sourceCmd.CommandText = string.Format(selectColumns, sourceTableName);
            targetCmd.CommandText = string.Format(selectColumns, targetTableName);

            if (mapTable.SourceTableColumns == null)
            {
                mapTable.SourceTableColumns = new List<MapColumn>();
                using (SqlDataReader reader = sourceCmd.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    DataTable dd = reader.GetSchemaTable();
                    foreach (DataRow dr in dd.Rows)
                    {
                        mapTable.SourceTableColumns.Add(new MapColumn() { Name = dr["ColumnName"].ToString(), ColumnType = Type.GetType(dr["DataType"].ToString(), true, true), IsPrimaryKey = bool.Parse(dr["IsKey"].ToString()) });
                    }
                }
            }
            if (mapTable.TargetTableColumns == null)
            {
                mapTable.TargetTableColumns = new List<MapColumn>();
                using (SqlDataReader reader = targetCmd.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    DataTable dd = reader.GetSchemaTable();
                    foreach (DataRow dr in dd.Rows)
                    {
                        mapTable.TargetTableColumns.Add(new MapColumn() { Name = dr["ColumnName"].ToString(), ColumnType = Type.GetType(dr["DataType"].ToString(), true, true), IsPrimaryKey = bool.Parse(dr["IsKey"].ToString()) });
                    }
                }
            }
            int c1 = mapTable.SourceTableColumns.Count;
            int c2 = mapTable.TargetTableColumns.Count;
            bool isSame = true;
            if (c1 == c2)
            {
                var plist1 = mapTable.SourceTableColumns.Where(p => p.IsPrimaryKey);
                var plist2 = mapTable.TargetTableColumns.Where(p => p.IsPrimaryKey);

                int pc1 = plist1.Count();
                int pc2 = plist2.Count();
                if (pc1 == pc2)
                {
                    for (int i = 0; i < pc1; i++)
                    {
                        var item1 = plist1.ElementAt(i);
                        var item2 = plist2.ElementAt(i);
                        if (item1.IsPrimaryKey != item2.IsPrimaryKey || item1.ColumnType != item2.ColumnType || !item1.Name.Equals(item2.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            isSame = false;
                            break;
                        }
                    }
                }
                else
                {
                    isSame = false;
                }
            }
            else
            {
                isSame = false;
            }

            return isSame;
        }



        private bool SyncTable(SqlDataAdapter sourceAda, SqlDataAdapter targetAda, SyncMap mapTable, SqlConnection sourceConnection, SqlConnection targetConnection)
        {
            bool isSync = false;

            string sourceTabName = mapTable.SourceTableName;
            string targetTabName = mapTable.TargetTableName;
            

            sourceTabName = formatTableName(mapTable, sourceTabName);
            targetTabName = formatTableName(mapTable, targetTabName);

           
            WriteLog(string.Format("<----开始同步{0}表---->", targetTabName));


            SqlCommand sourceCmd = sourceAda.SelectCommand;
            SqlCommand targetCmd = targetAda.SelectCommand;

            SqlCommand targetInsertCmd = new SqlCommand();
            SqlCommand targetUpdateCmd = new SqlCommand();
            SqlCommand targetDeleteCmd = new SqlCommand();
            //---------------------------------------------

            sourceCmd.Connection = sourceConnection;
            targetCmd.Connection = targetConnection;

            targetAda.InsertCommand = targetInsertCmd;
            targetAda.UpdateCommand = targetUpdateCmd;
            targetAda.DeleteCommand = targetDeleteCmd;

            targetInsertCmd.Connection = targetConnection;
            targetUpdateCmd.Connection = targetConnection;
            targetDeleteCmd.Connection = targetConnection;

            //同步列
            if (!SyncColumns(sourceCmd, targetCmd, mapTable, sourceTabName, targetTabName))
            {
                WriteLog("主键类型不一致");
                isSync = false;
            }

            bool isCheckTableSchema = mapTable.IsCheckTableSchema;
            bool isDeleteTargetRow = mapTable.IsDeleteTargetRow;
            bool isAddSync = mapTable.IsAddSync;
            //第二次查询记录条数
            sourceCmd.CommandText = string.Format(countSql, sourceTabName);
            targetCmd.CommandText = string.Format(countSql, targetTabName);

            object o1 = null, o2 = null;
            try
            {
                o1 = sourceCmd.ExecuteScalar();
                o2 = targetCmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                isSync = false;
            }

            long c1 = o1 == null || o1 is DBNull ? 0 : long.Parse(o1.ToString());
            long c2 = o2 == null || o2 is DBNull ? 0 : long.Parse(o2.ToString());

            if (c1 > 0)
            {
                int pageCount = (int)Math.Ceiling(c1 / (double)pageSize);
                int index = 0;
                //是否增量同步
                if (isAddSync)
                {
                    targetCmd.CommandText = string.Format(countSql, targetTabName);
                    index = c1 == c2 ? pageCount : (int)Math.Floor(c2 / (double)pageSize);
                }
                bool isFirst = true;
                for (int i = index; i < pageCount; i++)
                {
                    long startIndex = i * pageSize + 1;

                    long endIndex1 = (i + 1) * pageSize > c1 ? c1 : i * pageSize + pageSize;

                    long endIndex2 = startIndex + pageSize > c2 ? c2 : endIndex1;

                    #region 填充数据
                    //第三次查询分页记录
                    sourceCmd.CommandText = string.Format(selectSql, string.Join(",", mapTable.SourceTableColumns.FindAll(p => p.IsPrimaryKey == true).Select(x => x.Name)), sourceTabName, startIndex, endIndex1);
                    targetCmd.CommandText = string.Format(selectSql, string.Join(",", mapTable.TargetTableColumns.FindAll(p => p.IsPrimaryKey == true).Select(x => x.Name)), targetTabName, startIndex, endIndex2);

                    DataTable sourceTable = new DataTable(sourceTabName);
                    sourceAda.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                    sourceAda.Fill(sourceTable);

                    DataTable targetTable = new DataTable(targetTabName);
                    targetAda.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                    targetAda.Fill(targetTable);
                    #endregion

                    #region copy Data
                    if (isCheckTableSchema)
                    {
                        if (CompareTableSchema(sourceTable, targetTable))
                        {
                            CopyDataTable(sourceTable, targetTable, isDeleteTargetRow);
                        }
                        else
                        {
                            WriteLog(string.Format("{0}与{1}表结构不一致", sourceTabName, targetTabName));
                        }
                    }
                    else
                    {
                        CopyDataTable(sourceTable, targetTable, isDeleteTargetRow);
                    }
                    #endregion

                    if (isFirst)
                    {
                        initParameter(targetTable, targetInsertCmd, targetUpdateCmd, targetDeleteCmd);
                        isFirst = false;
                    }

                    #region sqlCommandBuilder
                    //SqlCommandBuilder scb = new SqlCommandBuilder(targetAda);
                    //scb.ConflictOption = ConflictOption.OverwriteChanges;
                    //targetAda.InsertCommand = scb.GetInsertCommand();
                    //targetAda.UpdateCommand = scb.GetUpdateCommand();
                    //targetAda.DeleteCommand = scb.GetDeleteCommand();
                    #endregion

                    DataTable tempTable = targetTable.GetChanges();
                    if (tempTable != null)
                    {
                        int rowCount = tempTable.Rows.Count;
                        //syncCount += rowCount;
                        targetAda.Update(tempTable);

                        targetTable.AcceptChanges();
                        string logTxt = string.Format("     同步{0}到{1}行的{2}条数据", startIndex, endIndex1, rowCount);
                        WriteLog(logTxt);
                        isSync = true;
                    }
                    else
                    {
                        string logTxt = string.Format("     同步{0}到{1}行的{2}条数据", startIndex, endIndex1, 0);
                        WriteLog(logTxt);
                    }
                }
            }
            if (mapTable.SubTimeSpan > 0)
            {
                mapTable.SubTimeSpan--;
                SyncTable(sourceAda, targetAda, mapTable, sourceConnection, targetConnection);
            }

            WriteLog(string.Format("<----同步{0}表结束---->", targetTabName));
            return isSync;
        }

       

        private DbType GetDBType(Type theType)
        {
            //System.ComponentModel.TypeConverter tc = System.ComponentModel.TypeDescriptor.GetConverter(idbDataParameter.DbType);
            //if (tc.CanConvertFrom(theType))
            //{
            //    idbDataParameter.DbType = (DbType)tc.ConvertFrom(theType.Name);
            //}
            //else
            //{
            //    try
            //    {
            //        idbDataParameter.DbType = (DbType)tc.ConvertFrom(theType.Name);
            //    }
            //    catch (Exception) { }
            //}
            //return idbDataParameter.DbType;
            SqlParameter p1 = new SqlParameter();
            TypeConverter tc = TypeDescriptor.GetConverter(p1.DbType);
            if (tc.CanConvertFrom(theType))
            {
                p1.DbType = (DbType)tc.ConvertFrom(theType.Name);
            }
            else
            {
                try
                {
                    p1.DbType = (DbType)tc.ConvertFrom(theType.Name);
                }
                catch
                {
                }
            }
            return p1.DbType;
        }

        /// <summary>
        /// 设置目标表的sql语句
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="insertCMD"></param>
        /// <param name="updateCMD"></param>
        /// <param name="deleteCMD"></param>
        public void initParameter(DataTable dt, SqlCommand insertCMD, SqlCommand updateCMD, SqlCommand deleteCMD)
        {
            insertCMD.Parameters.Clear();
            updateCMD.Parameters.Clear();
            deleteCMD.Parameters.Clear();

            if (dt.Columns.Contains("RowIndex"))
            {
                dt.Columns.Remove("RowIndex");
            }

            setInsertSQL(dt, insertCMD);

            setUpdateSQL(dt, updateCMD);

            setDeleteSQL(dt, deleteCMD);

            foreach (DataColumn dc in dt.Columns)
            {
                string columnName = dc.ColumnName;

                var para1 = new SqlParameter("@" + columnName, columnName);
                para1.SourceVersion = DataRowVersion.Current;
                para1.SourceColumn = columnName;
                para1.DbType = GetDBType(dc.DataType);
                para1.IsNullable = dc.AllowDBNull;

                var para2 = new SqlParameter("@" + columnName, columnName);
                para2.SourceVersion = DataRowVersion.Current;
                para2.SourceColumn = columnName;
                para2.DbType = GetDBType(dc.DataType);
                para2.IsNullable = dc.AllowDBNull;

                var para3 = new SqlParameter("@" + columnName, columnName);
                para3.SourceVersion = DataRowVersion.Current;
                para3.SourceColumn = columnName;
                para3.DbType = GetDBType(dc.DataType);
                para3.IsNullable = dc.AllowDBNull;

                //listParas1.Add(para1);
                //listParas2.Add(para2);
                //listParas3.Add(para3);
                insertCMD.Parameters.Add(para1);
                updateCMD.Parameters.Add(para2);
                deleteCMD.Parameters.Add(para3);

            }

            insertCMD.UpdatedRowSource = UpdateRowSource.None;
            updateCMD.UpdatedRowSource = UpdateRowSource.None;
            deleteCMD.UpdatedRowSource = UpdateRowSource.None;

        }

        public void setInsertSQL(DataTable dt, SqlCommand insertCmd)
        {
            string tabName = dt.TableName;
            string key = tabName + "_insert";
            if (hashTable.Contains(key))
            {
                insertCmd.CommandText = hashTable[key].ToString();
            }
            else
            {
                List<string> aklist1 = new List<string>(); //@id,@name
                List<string> aklist2 = new List<string>(); //id,name 

                //主键列表
                List<string> pklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> pklist2 = new List<string>(); //id,name

                //除主键列外所有
                List<string> fklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> fklist2 = new List<string>(); //id,name

                //除自增列外所有
                List<string> zklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> zklist2 = new List<string>(); //id,name

                //主键是否包含自增列
                bool isPrimaryKeyAutoIncrement = false;
                if (dt.PrimaryKey.Length > 0)
                {
                    foreach (DataColumn dc in dt.PrimaryKey)
                    {
                        string columnName = dc.ColumnName;
                        pklist1.Add(columnName + "=@" + columnName);
                        pklist2.Add("[" + columnName + "]");
                        if (dc.AutoIncrement)
                        {
                            isPrimaryKeyAutoIncrement = true;
                        }
                    }
                }

                foreach (DataColumn dc in dt.Columns)
                {
                    string columnName = dc.ColumnName;
                    if (dc.AutoIncrement)
                    {
                        //若没有设置主键，用自增列作为主键
                        if (pklist1.Count == 0)
                        {
                            isPrimaryKeyAutoIncrement = true;
                            pklist1.Add("[" + columnName + "]=@" + columnName);
                            pklist2.Add("[" + columnName + "]");
                        }
                    }
                    else
                    {
                        //非自增列
                        zklist1.Add("@" + columnName);
                        zklist2.Add("[" + columnName + "]");
                    }

                    //非主键列
                    if (!pklist2.Contains("[" + columnName + "]"))
                    {
                        fklist1.Add("[" + columnName + "]=@" + columnName);
                        fklist2.Add("[" + columnName + "]");
                    }
                    //所有列
                    aklist1.Add("@" + columnName);
                    aklist2.Add("[" + columnName + "]");
                }

                if (pklist1.Count == 0)
                {
                    throw new Exception("没有定义主键列");
                }

                string s1, s2;
                if (!isPrimaryKeyAutoIncrement)
                {
                    s1 = string.Join(", ", aklist2.ToArray());
                    s2 = string.Join(", ", aklist1.ToArray());
                }
                else
                {
                    s1 = string.Join(", ", zklist2.ToArray());
                    s2 = string.Join(", ", zklist1.ToArray());
                }
                string sql = string.Format(insertSql, tabName, s1, s2);
                insertCmd.CommandText = sql;
                hashTable[key] = sql;
            }
        }

        public void setUpdateSQL(DataTable dt, SqlCommand updateCmd)
        {
            string tabName = dt.TableName;
            string key = tabName + "_update";
            if (hashTable.Contains(key))
            {
                updateCmd.CommandText = hashTable[key].ToString();
            }
            else
            {
                List<string> aklist1 = new List<string>(); //@id,@name
                List<string> aklist2 = new List<string>(); //id,name 

                //主键列表
                List<string> pklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> pklist2 = new List<string>(); //id,name

                //除主键列外所有
                List<string> fklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> fklist2 = new List<string>(); //id,name

                if (dt.PrimaryKey.Length > 0)
                {
                    foreach (DataColumn dc in dt.PrimaryKey)
                    {
                        string columnName = dc.ColumnName;
                        pklist1.Add("[" + columnName + "]=@" + columnName);
                        pklist2.Add("[" + columnName + "]");
                    }
                }

                foreach (DataColumn dc in dt.Columns)
                {
                    string columnName = dc.ColumnName;
                   
                    if (dc.AutoIncrement)
                    {
                        //若没有设置主键，用自增列作为主键
                        if (pklist1.Count == 0)
                        {
                            pklist1.Add("[" + columnName + "]=@" + columnName);
                            pklist2.Add("[" + columnName + "]");
                        }
                    }
                    //非主键列
                    if (!pklist2.Contains("[" + columnName + "]"))
                    {
                        fklist1.Add("[" + columnName + "]=@" + columnName);
                        fklist2.Add("[" + columnName + "]");
                    }
                    //所有列
                    aklist1.Add("@" + columnName);
                    aklist2.Add("[" + columnName + "]");
                }

                if (pklist1.Count == 0)
                {
                    throw new Exception("没有定义主键列");
                }

                string s1 = "", s2 = "";
                if (pklist1.Count > 0)
                {
                    s1 = string.Join(", ", fklist1.ToArray());
                    s2 = string.Join(" and ", pklist1.ToArray());
                }
                string sql = string.Format(updateSql, tabName, s1, s2);
                updateCmd.CommandText = sql;
                hashTable[key] = sql;
            }
        }

        public void setDeleteSQL(DataTable dt, SqlCommand deleteCmd)
        {
            string tabName = dt.TableName;
            string key = tabName + "_delete";
            if (hashTable.Contains(key))
            {
                deleteCmd.CommandText = hashTable[key].ToString();
            }
            else
            {
                //主键列表
                List<string> pklist1 = new List<string>(); //Id=@Id, Name=@Name
                List<string> pklist2 = new List<string>(); //id,name

                if (dt.PrimaryKey.Length > 0)
                {
                    foreach (DataColumn dc in dt.PrimaryKey)
                    {
                        string columnName = dc.ColumnName;
                        pklist1.Add("[" + columnName + "]=@" + columnName);
                        pklist2.Add("[" + columnName + "]");
                    }
                }

                foreach (DataColumn dc in dt.Columns)
                {
                    string columnName = dc.ColumnName;
                    if (dc.AutoIncrement)
                    {
                        //isHasAutoIncrement = true;
                        //若没有设置主键，用自增列作为主键
                        if (pklist1.Count == 0)
                        {
                            pklist1.Add("[" + columnName + "]=@" + columnName);
                            pklist2.Add("[" + columnName + "]");
                        }
                    }
                }

                if (pklist1.Count == 0)
                {
                    throw new Exception("没有定义主键列");
                }

                string s1 = "";
                if (pklist1.Count > 0)
                {
                    s1 = string.Join(" and ", pklist1.ToArray());
                }
                string sql = string.Format(deleteSql, tabName, s1);
                deleteCmd.CommandText = sql;
                hashTable[key] = sql;
            }
        }

        public bool CompareTableSchema(DataTable dtSrc, DataTable dtTarget)
        {
            bool isSame = true;
            if (dtSrc.Columns.Count == dtTarget.Columns.Count)
            {
                for (int i = 0, count = dtSrc.Columns.Count; i < count; i++)
                {
                    var c1 = dtSrc.Columns[i];
                    var c2 = dtTarget.Columns[i];
                    if (c1.DataType != c2.DataType)
                    {
                        isSame = false;
                        break;
                    }
                }
            }
            else
            {
                isSame = false;
            }
            return isSame;

        }

        /// <summary>
        /// 仅限于主键非字符串的写法
        /// </summary>
        /// <param name="pcs"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        //public string getSql(DataColumn[] pcs, DataRow row)
        //{
        //    StringBuilder sb = new StringBuilder();
        //    for (int i = 0, count = pcs.Length; i < count; i++)
        //    {
        //        string columnName = pcs[i].ColumnName;
        //        if (i < count - 1)
        //        {
        //            sb.Append(columnName + "={" + row[columnName] + "} and");
        //        }
        //        else
        //        {
        //            sb.Append(columnName + "={" + row[columnName] + "}");
        //        }
        //    }
        //    return sb.ToString();
        //}

        public object[] getSql(DataColumn[] pcs, DataRow row)
        {
            object[] objs = new object[pcs.Length];
            for (int i = 0, count = pcs.Length; i < count; i++)
            {
                objs[i] = row[pcs[i].ColumnName];
            }
            return objs;
        }

        /// <summary>
        /// copy table A 到 table B
        /// </summary>
        /// <param name="srcTab">table A </param>
        /// <param name="targetTab"> table B</param>
        private void CopyDataTable(DataTable tableA, DataTable tableB, bool IsDeleteTargetRow = false)
        {
            //tableB.AcceptChanges(); //这个是关键
            tableB.BeginLoadData();
            int c1 = tableA.Rows.Count;
            int c2 = tableB.Rows.Count;
            if (IsDeleteTargetRow)
            {
                var pks = tableB.PrimaryKey.Select<DataColumn, string>(p => p.ColumnName);
                int len = pks.Count();
                //遍历TableB里的在tableA中不存在的数据行删除掉
                for (int i = 0; i < c2; i++)
                {
                    DataRow dr = tableB.Rows[i];
                    object[] objs = new object[len];
                    for (int x = 0; x < len; x++)
                    {
                        objs[x] = dr[pks.ElementAt(x)];
                    }
                    //var rr = tableA.Rows.Find(objs);
                    if (!tableA.Rows.Contains(objs))
                    {
                        tableB.Rows[i].Delete();
                    }
                }
                for (int i = 0; i < c1; i++)
                {
                    DataRow dr = tableA.Rows[i];
                    tableB.LoadDataRow(dr.ItemArray, LoadOption.Upsert);
                }
            }
            else
            {
                for (int i = 0; i < c1; i++)
                {
                    //OverwriteChanges 用输入行的值分别更新该行的当前版本和原始版本
                    //PreserveChanges 默认选项。用输入行的值更新该行的原始版本
                    //Upsert 用输入行的值更新该行的当前版本
                    tableB.LoadDataRow(tableA.Rows[i].ItemArray, LoadOption.Upsert);
                }
            }
          
            tableB.EndLoadData();

            
            
        }


      


    }
}
