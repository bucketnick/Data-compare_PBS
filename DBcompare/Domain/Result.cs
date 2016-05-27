using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
    public class Result
    {
        public Result()
        {
            Concept = string.Empty;
            PKColumnName = string.Empty;
            PKValue = string.Empty;
            ColumnName = string.Empty;
            ReferenceDBValue = string.Empty;
            DestinationDBValue = string.Empty;
            ReferenceName = string.Empty;
        }
        public string Concept { get; set; }

        public string PKColumnName { get; set; }

        public string PKValue { get; set; }

        public string ColumnName { get; set; }

        public string ReferenceDBValue { get; set; }

        public string DestinationDBValue { get; set; }

        public string ReferenceName { get; set; }

    }
}
