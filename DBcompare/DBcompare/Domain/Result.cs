using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
    public class Result
    {
        public string Concept { get; set; }

        public string PKColumnName { get; set; }

        public string PKValue { get; set; }

        public string ColumnName { get; set; }

        public string ReferenceDBValue { get; set; }

        public string DestinationDBValue { get; set; }


    }
}
