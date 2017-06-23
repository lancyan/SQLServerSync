using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZD.SyncDB
{
    public class SyncMap
    {


        public string SourceTableName;

        public string TargetTableName;

        public List<string> SourceTableColumns;

        public List<string> TargetTableColumns;



        /// <summary>
        /// 0代表source-->target 单向同步
        /// 1代表target-->source 单向同步
        /// 2代表 双向同步
        /// </summary>
        public int Direction { get; set; }

        /// <summary>
        /// 是否增量同步
        /// </summary>
        public bool IsAddSync { get; set; }

        /// <summary>
        /// 是否检查表结构一致性
        /// </summary>
        public bool IsCheckTableSchema { get; set; }

        /// <summary>
        /// 是否删除源表中不存在，而目标表中存在的数据
        /// </summary>
        public bool IsDeleteTargetRow { get; set; }
  
    }
}
