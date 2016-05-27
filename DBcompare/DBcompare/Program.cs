using System;
using System.Collections.Generic;
using System.Text;
using Common.Logging;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Linq;
using DBComparer;


namespace DBcomparer
{
    class Program
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static void Main(string[] args)
        {
            bool isAU = bool.Parse(ConfigurationSettings.AppSettings["IsAU"].ToString());
            log.Info("----DB comparer Job Start---");
            if (isAU)
                Application.AUDBcomparer();
            else
                Application.AsiaDBcomparer();
            log.Info("----DB comparer Job End---");
        }
     

    }
}