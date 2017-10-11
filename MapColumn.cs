using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZD.SyncDB
{
    public class MapColumn
    {
        public string Name { get; set; }

        public bool IsPrimaryKey { get; set; }

        public Type ColumnType { get; set; }

    }
}
