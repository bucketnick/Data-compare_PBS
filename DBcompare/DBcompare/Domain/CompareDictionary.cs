using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
     public class CompareDictionary
    {
        public string Key {get; set;}
        public string ToCompareValue { get; set; }
        public string ReferenceValue { get; set; }
    }
}
