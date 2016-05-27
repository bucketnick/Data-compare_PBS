using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Common.Logging;
using DBComparer.Domain;

namespace DBComparer
{
    public static class DBHelper
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        public static void LogActivity(string InsertSql)
        {
            ExecuteScript(InsertSql, LogDBConnectionStr);
        }


        #region Data Access
        public static List<Mapping> GetMappingList(string script)
        {
            List<Mapping> mapping = new List<Mapping>();
            DataTable dt = GetDataTable(script, LogDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Mapping mp = new Mapping();
                mp.Concept = dt.Rows[i][0].ToString();
                mp.Field = dt.Rows[i][1].ToString();
                mp.IsPK = int.Parse(dt.Rows[i][2].ToString());
                mapping.Add(mp);
            }
            return mapping;

        }
        public static List<Mapping> GetAsiaMappingList()
        {
            string script = "select * from dbo.CountryTemplateMapping order by TableName";
            List<Mapping> mapping = new List<Mapping>();
            DataTable dt = GetDataTable(script, LogDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Mapping mp = new Mapping();
                mp.Concept = dt.Rows[i][0].ToString();
                mp.Field = dt.Rows[i][1].ToString();
                mp.IsPK = int.Parse(dt.Rows[i][2].ToString());
                mp.referenceField = dt.Rows[i][3].ToString();
                
                mapping.Add(mp);
            }
            return mapping;

        }

        public static void CreateTable(string tablename)
        {
            string createtable = @"
CREATE TABLE [dbo].["+tablename+ @"](
	[Concept] [NVARCHAR](max) NULL,
	[PKColumnName] [NVARCHAR](max) NULL,
	[PKValue] [NVARCHAR](max) NULL,
	[ColumnName] [NVARCHAR](max) NULL,
	[ADHOC] [NVARCHAR](MAX) NULL,
	[UAT] [NVARCHAR](MAX) NULL,
[Reference] [NVARCHAR](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]";

            ExecuteScript(createtable, LogDBConnectionStr);

        }

   


        public static List<SpecialMapping> GetSpecialMappingList(string script)
        {
            List<SpecialMapping> specialmapping = new List<SpecialMapping>();
            DataTable dt= GetDataTable(script, DestinationDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                SpecialMapping smp = new SpecialMapping();
                smp.Concept = dt.Rows[i][0].ToString();
                smp.ReferenceDBColumnname = dt.Rows[i][1].ToString();
                smp.DestionationDBColumnname = dt.Rows[i][2].ToString();
                smp.Key = dt.Rows[i][3].ToString();
                specialmapping.Add(smp);
            }

            return specialmapping;

        }


        public static List<ComplexPKMapping> GetComplexPKMappingList(string script)
        {
            List<ComplexPKMapping> mapping = new List<ComplexPKMapping>();
            DataTable dt= GetDataTable(script, DestinationDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ComplexPKMapping mp = new ComplexPKMapping();
                mp.MainConcept = dt.Rows[i][0].ToString();
                mp.MaiKey = dt.Rows[i][1].ToString();
                mp.LinkedConcept = dt.Rows[i][2].ToString();
                mp.LinkedKey = dt.Rows[i][3].ToString();
                mapping.Add(mp);
            }
            return mapping;

        }

        public static List<CompareDictionary> GetCompareResultList(string script)
        {
            List<CompareDictionary> cdList = new List<CompareDictionary>();
            DataTable dt = GetDataTable(script, DestinationDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                CompareDictionary cd = new CompareDictionary();
                cd.Key = dt.Rows[i][0].ToString();
                cd.ToCompareValue = dt.Rows[i][1] == null ? string.Empty : dt.Rows[i][1].ToString();
                cd.ReferenceValue =  dt.Rows[i][2]==null?string.Empty:dt.Rows[i][2].ToString();
                cdList.Add(cd);
            }
            return cdList;

        }


        public static List<Address> GetAddressList(string script)
        {
            List<Address> adList = new List<Address>();
            DataTable dt = GetDataTable(script, DestinationDBConnectionStr);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                Address ad = new Address();
                ad.LegacyId = dt.Rows[i][0].ToString();
                ad.Tocompare_Address1 = dt.Rows[i][1] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Tocompare_Address2 = dt.Rows[i][2] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Tocompare_Address3 = dt.Rows[i][3] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Tocompare_PostalCode = dt.Rows[i][4] == null ? string.Empty : dt.Rows[i][1].ToString();


                ad.Reference_Address1 = dt.Rows[i][6] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Reference_Address2 = dt.Rows[i][7] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Reference_Address3 = dt.Rows[i][8] == null ? string.Empty : dt.Rows[i][1].ToString();
                ad.Reference_PostalCode = dt.Rows[i][9] == null ? string.Empty : dt.Rows[i][1].ToString();

                adList.Add(ad);
            }

            return adList;

        }


        public static int GetDestinationDBDataTableCount(string script)
        {
            return GetDataTableCount(script, DestinationDBConnectionStr);

        }

        public static DataTable GetReferenceDBDataTable(string script)
        {
            return GetDataTable(script, ReferenceDBConnectionStr);
        }

        public static int GetReferenceDBDataTableCount(string script)
        {
            return GetDataTableCount(script, ReferenceDBConnectionStr);

        }

        public static int GetTableCount(string script,bool IsReferenceDB,bool IsDestinationDB)
        {
            if(IsReferenceDB)
                return GetDataTableCount(script, ReferenceDBConnectionStr);

            if (IsDestinationDB)
                return GetDataTableCount(script, DestinationDBConnectionStr);


                return 0;
        }
        

        public static DataSet GetDataSet(string script)
        {
            return GetDataSet(script, ReferenceDBConnectionStr);
        }

        public static DataTable GetDataTable(string script, string connectionString)
        {
            DataTable table = new DataTable();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(script, connection);
                command.CommandTimeout = 0;
                SqlDataAdapter adapter = new SqlDataAdapter(command);

                connection.Open();
                adapter.Fill(table);
            }

            return table;
        }

        public static int GetDataTableCount(string script, string connectionString)
        {
            DataTable table = new DataTable();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(script, connection);
                command.CommandTimeout = 0;
                SqlDataAdapter adapter = new SqlDataAdapter(command);

                connection.Open();
                adapter.Fill(table);
            }

            return table.Rows.Count;
        }



        public static DataSet GetDataSet(string script, string connectionString)
        {
            DataSet dataSet = new DataSet();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(script, connection);
                SqlDataAdapter adapter = new SqlDataAdapter(command);

                connection.Open();
                adapter.Fill(dataSet);
            }

            return dataSet;
        }

        public static void ExecuteScript(string script, string connectionString)
        {
            //script.Replace("'", "''''");
            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand())
            {
                command.Connection = connection;
                connection.Open();

                command.CommandText = script;
                command.ExecuteNonQuery();
            }
        }
        #endregion

        #region Connection String
        public static string DestinationDBConnectionStr
        {
            get
            {
                return ConfigurationSettings.AppSettings["DestinationDB_ConnectionString"].ToString();
            }
        }
        public static string ReferenceDBConnectionStr
        {

            get
            {
                return ConfigurationSettings.AppSettings["ReferenceDB_ConnectionString"].ToString();

            }

        }

        public static string LogDBConnectionStr
        {

            get
            {
                return ConfigurationSettings.AppSettings["MappingDB_ConnectionString"].ToString();

            }

        }


        #endregion
    }
}






