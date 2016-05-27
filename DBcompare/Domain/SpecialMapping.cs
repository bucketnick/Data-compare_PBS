using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
    public class SpecialMapping
    {

        public string Concept {get; set;}

        public string ReferenceDBColumnname {get; set;}

        public string DestionationDBColumnname {get; set;}

        public string Key { get; set;}

    }
}
