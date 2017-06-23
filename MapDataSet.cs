using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZD.SyncDB
{
    public class MapDataSet
    {
        public string DataSetName { get; set; }

        //public string targetDatasetName { get; set; }
        //public string targetConnectString { get; set; }

        public string ConnectString { get; set; }

        public List<MapDataTable> MapTables { get; set; }

        //public int syncType { get; set; }


    }
}
