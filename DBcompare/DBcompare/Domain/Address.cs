using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBComparer.Domain
{
    public class Address
    {
        public string LegacyId {get;set;}
        public string Tocompare_Address1 {get; set;}
        public string Tocompare_Address2 {get; set;}
        public string Tocompare_Address3 { get; set; }
        public string Tocompare_PostalCode { get; set; }

        public string Reference_Address1 { get; set; }
        public string Reference_Address2 { get; set; }
        public string Reference_Address3 { get; set; }
        public string Reference_PostalCode { get; set; }


    }
}
