using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
    public class Mapping
    {
        public string Concept { get; set; }
        public string Field { get; set; }
        public int IsPK { get; set; }
        public string referenceField { get; set; }
    }
}
