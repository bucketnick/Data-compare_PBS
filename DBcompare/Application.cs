using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBComparer;
using System.Data;
using System.Configuration;
using Common.Logging;
using DBComparer.Domain;
using System.Data.SqlClient;

namespace DBComparer
{
    public static class Application
    {
        #region Property

        public static string ReferenceDBName = ConfigurationSettings.AppSettings["ReferenceDBName"].ToString();
        public static string TestDBName = ConfigurationSettings.AppSettings["DestinationDBName"].ToString();
        public static string CountryCode = ConfigurationSettings.AppSettings["CountryCode"].ToString();
        public static string AppendText = ConfigurationSettings.AppSettings["AppendName"].ToString();

        public static List<Mapping> MappingList = DBHelper.GetAsiaMappingList();
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static string CompareTableResult= string.Format("CompareResult_{0}_{1}_{2}_{3}{4}{5}",
                            CountryCode,
                            AppendText,
                            DateTime.Now.Year,
                            DateTime.Now.Month < 10 ? string.Format("0{0}", DateTime.Now.Month) : "" + DateTime.Now.Month,
                            DateTime.Now.Day, DateTime.Now.Minute);
        #endregion

        #region Constructor

        public static void AUDBcomparer()
        {
            // read mapping talble    
            string ReferenceDBname = ConfigurationSettings.AppSettings["ReferenceDBName"].ToString();
            string TocompareDBname = ConfigurationSettings.AppSettings["DestinationDBName"].ToString();
            string retrieveMappingtablewithPK = "select * from  TableNamePKMapping order by TableName";
            List<Mapping> mapping = DBHelper.GetMappingList(retrieveMappingtablewithPK);

            #region check the normal mapping files
            foreach (var mp in mapping.Where(o => o.IsPK == 1))
            {
                string concept = mp.Concept;    //table name
                string pkcolunmn = mp.Field;    //PK column name


                string retrieveTableCount = @"select " + pkcolunmn + " From " + concept;

                int DestinationDBtablecount = DBHelper.GetDestinationDBDataTableCount(retrieveTableCount);
                int ReferenceDBTablecount = DBHelper.GetReferenceDBDataTableCount(retrieveTableCount);
                Result logresult = new Result();
                logresult.Concept = concept;
                logresult.DestinationDBValue = "Table count is:" + DestinationDBtablecount.ToString();
                logresult.ReferenceDBValue = "Table count is:" + ReferenceDBTablecount.ToString();

                log.Info("Log Table Level Info");

                string insertlog = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                    + " ('{0}','{1}','{2}','{3}','{4}','{5}')", logresult.Concept, null, null, null, logresult.ReferenceDBValue, logresult.DestinationDBValue);

                DBHelper.LogActivity(insertlog);


                string retrieveMatchedRecordsCountbyPK =
                     string.Format(@"select a.{0} from {1}.dbo.{2} a inner join [10.10.10.31\INTERIMDB].{3}.dbo.{2} b
                        on a.{0}= b.{0} ", pkcolunmn, TocompareDBname, concept, ReferenceDBname);

                //                string retrieveMatchedRecordsCountbyPK =
                //                    string.Format(@"select a.{0} from {1}.dbo.{2} a inner join {3}.dbo.{2} b
                //                        on a.{0}= b.{0} ", pkcolunmn, TocompareDBname, concept, ReferenceDBname);

                log.Info("Log Table Level Info on matched Records");
                int matchedRecordscount = DBHelper.GetDestinationDBDataTableCount(retrieveMatchedRecordsCountbyPK);
                Result logresultMatchedRecords = new Result();

                logresultMatchedRecords.Concept = concept;
                if (matchedRecordscount > 0)
                {
                    logresultMatchedRecords.DestinationDBValue = "Total matched records by PK is :" + matchedRecordscount.ToString();
                    logresultMatchedRecords.ReferenceDBValue = "Total matched records by PK is :" + matchedRecordscount.ToString();

                }
                else
                {
                    logresultMatchedRecords.DestinationDBValue = "No matched records by PK!";
                    logresultMatchedRecords.ReferenceDBValue = "No matched records by PK!";
                }

                string insertlogcount = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                + " ('{0}','{1}','{2}','{3}','{4}','{5}')", logresultMatchedRecords.Concept, null, null, null, logresultMatchedRecords.ReferenceDBValue, logresultMatchedRecords.DestinationDBValue);

                DBHelper.LogActivity(insertlogcount);

                foreach (var mp_excludePK in mapping.Where(o => o.IsPK == 0 && o.Concept == concept))
                {
                    string columnname = mp_excludePK.Field;
                    List<CompareDictionary> cdList = new List<CompareDictionary>();

                    //get the list of CompareDictionary 
                    if (concept != "Monograph")
                    {
                        string retrievePKValue_ReferenceDBValue_DestinationDBValue =
                     string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [10.10.10.31\INTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        cdList = DBHelper.GetCompareResultList(retrievePKValue_ReferenceDBValue_DestinationDBValue);

                    }

                    else
                    {
                        string retrievePKValue_ReferenceDBValue_DestinationDBValue_BriefMonograph =
                        string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [10.10.10.31\INTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} where a.MonographTypeCT='58ecbf9d-c318-496a-98ec-a32100c7f139' and b.MonographTypeCT= '8e76b248-9f0f-4474-91f3-2bdc87c2a5c7' ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        cdList = DBHelper.GetCompareResultList(retrievePKValue_ReferenceDBValue_DestinationDBValue_BriefMonograph);


                        string retrievePKValue_ReferenceDBValue_DestinationDBValue_FullMonograph =
                        string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [10.10.10.31\INTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} where a.MonographTypeCT='8b29f44d-26c7-47c6-8a65-a32100c8190d' and b.MonographTypeCT= '3ea96813-3bae-464a-a1ed-be3736589701' ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        List<CompareDictionary> cdList_FullMonograph = DBHelper.GetCompareResultList(retrievePKValue_ReferenceDBValue_DestinationDBValue_FullMonograph);

                        cdList.AddRange(cdList_FullMonograph);



                    }




                    foreach (var cd in cdList)
                    {
                        if (!string.Equals(cd.ReferenceValue.Trim().ToUpper(), cd.ToCompareValue.Trim().ToUpper()))
                        {
                            Result Diff = new Result();
                            Diff.Concept = concept;
                            Diff.PKColumnName = pkcolunmn;
                            Diff.PKValue = cd.Key.ToString();
                            Diff.ColumnName = columnname;
                            Diff.DestinationDBValue = cd.ToCompareValue;
                            Diff.ReferenceDBValue = cd.ReferenceValue;
                            string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                 + " ('{0}','{1}','{2}','{3}','{4}','{5}')", Diff.Concept, Diff.PKColumnName, Diff.PKValue, Diff.ColumnName, Diff.ReferenceDBValue.Replace("'", "''"), Diff.DestinationDBValue.Replace("'", "''"));

                            DBHelper.LogActivity(insertDiff);


                        }

                    }

                    //to do


                }


            }

            #endregion

            //--handle special case


            #region check special table with the different mapping

            string retrieveSepecialMappingTablewithPK = "select * from SpecialFieldMapping";

            List<SpecialMapping> specialMapping = DBHelper.GetSpecialMappingList(retrieveSepecialMappingTablewithPK);
            foreach (var smp in specialMapping)
            {
                string sconcept = smp.Concept;
                string sreferencecolumnname = smp.ReferenceDBColumnname;
                string sdestinationcolumnname = smp.DestionationDBColumnname;
                string spk = smp.Key;

                List<CompareDictionary> scd = new List<CompareDictionary>();

                string sretrievePKValue_ReferenceDBValue_DestinationDBValue =
                 string.Format(@"select a.{0},a.{1}, b.{5} from {3}.dbo.{2} a inner join [10.10.10.31\INTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", spk, sdestinationcolumnname, sconcept, TocompareDBname, ReferenceDBname, sreferencecolumnname);
                List<CompareDictionary> scdList = DBHelper.GetCompareResultList(sretrievePKValue_ReferenceDBValue_DestinationDBValue);
                foreach (var scod in scdList)
                {

                    if (!(string.IsNullOrEmpty(scod.ReferenceValue) && scod.ToCompareValue == "False") && !string.Equals(scod.ReferenceValue.Trim().ToUpper(), scod.ToCompareValue.Trim().ToUpper()))
                    {
                        Result Diff = new Result();
                        Diff.Concept = sconcept;
                        Diff.PKColumnName = spk;
                        Diff.PKValue = scod.Key.ToString();
                        Diff.ColumnName = sdestinationcolumnname;
                        Diff.DestinationDBValue = scod.ToCompareValue;
                        Diff.ReferenceDBValue = scod.ReferenceValue;
                        string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
             + " ('{0}','{1}','{2}','{3}','{4}','{5}')", Diff.Concept, Diff.PKColumnName, Diff.PKValue, Diff.ColumnName, Diff.ReferenceDBValue.Replace("'", "''"), Diff.DestinationDBValue.Replace("'", "''"));

                        DBHelper.LogActivity(insertDiff);

                    }


                }



            }




            #endregion


            #region check  table with the complex PK

            string retrieveComplexPKMappingTable = "select * from ComplexPKMappingTable";

            List<ComplexPKMapping> ComplexPKMapping = DBHelper.GetComplexPKMappingList(retrieveComplexPKMappingTable);

            foreach (var cmp in ComplexPKMapping)
            {
                string mainconcept = cmp.MainConcept;
                string maikey = cmp.MaiKey;
                string linkedconcept = cmp.LinkedConcept;
                string linkedkey = cmp.LinkedKey;

                //build sql statment to retireve data

                if (linkedconcept == "Address")
                {
                    string retrieveAddressdatasql = "select * from (select b.LegacyId,a.Address1,a.Address2,a.Address3,a.PostalCode  from Address a inner join company b on a.companyid=b.id where a.IsMain=1 ) uat "
                        +
 " inner join  (select legacyId, Address1,Address2,Address3,PostalCode  from [10.10.10.31\\INTERIMDB].InterimDB_AU_20150121_120031_Release.dbo.address ) adhoc "
 +
"on uat.LegacyId= adhoc.LegacyId ";

                    List<Address> adList = new List<Address>();
                    adList = DBHelper.GetAddressList(retrieveAddressdatasql);

                    foreach (var ad in adList)
                    {
                        if (ad.Reference_Address1.Trim().ToUpper() != ad.Tocompare_Address1.Trim().ToUpper())
                        {
                            string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                + " ('Address','CompanyLegacyId +Ismain=1','{0}','Address1','{1}','{2}')", ad.LegacyId, ad.Reference_Address1.Replace("'", "''"), ad.Tocompare_Address1.Replace("'", "''"));
                            DBHelper.LogActivity(insertDiff);

                        }

                        if (ad.Reference_Address2.Trim().ToUpper() != ad.Tocompare_Address2.Trim().ToUpper())
                        {
                            string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                + " ('Address','CompanyLegacyId +Ismain=1','{0}','Address2','{1}','{2}')", ad.LegacyId, ad.Reference_Address2.Replace("'", "''"), ad.Tocompare_Address2.Replace("'", "''"));
                            DBHelper.LogActivity(insertDiff);

                        }


                        if (ad.Reference_Address3.Trim().ToUpper() != ad.Tocompare_Address3.Trim().ToUpper())
                        {
                            string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                + " ('Address','CompanyLegacyId +Ismain=1','{0}','Address3','{1}','{2}')", ad.LegacyId, ad.Reference_Address3.Replace("'", "''"), ad.Tocompare_Address3.Replace("'", "''"));
                            DBHelper.LogActivity(insertDiff);

                        }

                        if (ad.Reference_PostalCode.Trim().ToUpper() != ad.Tocompare_PostalCode.Trim().ToUpper())
                        {
                            string insertDiff = string.Format(@"insert into CompareResult (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT) values"
                + " ('Address','CompanyLegacyId +Ismain=1','{0}','PostalCode','{1}','{2}')", ad.LegacyId, ad.Reference_PostalCode.Replace("'", "''"), ad.Tocompare_PostalCode.Replace("'", "''"));
                            DBHelper.LogActivity(insertDiff);
                        }


                    }

                }

                else if (linkedconcept == "Contact")
                {
                    //use below script to check  , all records matched

                    //  select c.LegacyId, c.ContactType, b.ContactType, c.Data, b.Data from  

                    // ( select b.LegacyId,a.data, a.ContactType from  Contact a  inner join Company b on a.CompanyId= b.Id ) c

                    //inner join   [10.10.10.31\INTERIMDB].InterimDB_AU_20150121_120031_Release.dbo.Contact b

                    //on  c.LegacyId= b.legacyid and c.ContactType= b.contacttype

                    //where c.Data<> b.data

                }
            }

            #endregion

        }


        public static void AsiaDBcomparer()
        {
            try
            {


                    DBHelper.CreateTable(CompareTableResult);

                    #region check the normal mapping files
                    foreach (var mp in MappingList.Where(o => o.IsPK == 1 && o.Concept == "q_Prod_Local_Therap_Class"))
                    {
                        string concept = mp.Concept;    //table name
                        string keyColumn = mp.Field;    //PK column name
                        string referenceColumn = "";

                        //Get the reference field
                        if (MappingList.Any(x => x.Concept == mp.Concept && x.referenceField == "1"))
                        {
                            referenceColumn = MappingList.FirstOrDefault(x => x.Concept == mp.Concept && x.referenceField == "1").Field;
                        }

                        if (concept.Contains("Mapping") || concept.Contains("rel_")) continue;

                        //CHECK PK2 here only for those PK1 can't match             
                        if (!CheckPK2(keyColumn, DBHelper.DestinationDBConnectionStr, concept) && !CheckPK2(keyColumn, DBHelper.ReferenceDBConnectionStr, concept))
                            continue;

                        //-------------------
                        //Log Table count
                        //-------------------

                        CompareProcess(concept, keyColumn, referenceColumn);
                    }

                    #endregion
            }

            catch(Exception ex)
            {
                log.Info(ex.Message.ToString());
            
            }



        }

        #endregion


        #region Logic

        private static void CompareProcess(string concept, string keyColumn, string referenceColumn)
        {
            string compositeKey = ConstructCompositeKeyGrouping(concept);

            //1. Log the total count
            LogTableCount(concept, keyColumn);
            
            //2. Log the Match count including both PK1 and PK2
            // First, count the PK1 match = A
            // Second, count the PK2 match = B
            // The result should be A + B
            // Capture A set and B set IDs
            DataTable result = GenerateMatchRecords(concept, keyColumn, compositeKey);
            LogMatchedCount(concept, result.Rows.Count);


            //3. Generate the Match result
            // First, match by the PK1 then log all the field difference
            // Second, if not match by PK 1, then attemp to use PK 2 to match. PK2 must be 1-1 hence require some checking.
            GenerateMatchResult(result, concept, keyColumn, referenceColumn);

            //4. Generate the non-match
            // It the not match via PK1 or PK2. Capture both scenerio of Only In Old or Only In New
            // First, for only In Old, 
            // Second, for only in new
            //GenerateNonMatch(concept, keyColumn, compositeKey);

            //Generate the DataTable for the Asia
            DataTable referenceResult = GenerateRecords(concept,ReferenceDBName ,true);

            GenerateNonMatch(referenceResult, result, concept, keyColumn, referenceColumn,true);

            //Generate the DataTable for the Live
            DataTable testResult = GenerateRecords(concept, TestDBName,false);
            GenerateNonMatch(testResult, result, concept, keyColumn, referenceColumn,false);
        }

        private static void GenerateNonMatch(DataTable totalList, DataTable matchedList, string concept, string keyColumn, string referenceColumn,bool isReferenceDB)
        {

            string a_keyColumn = !isReferenceDB ? string.Format("a_{0}", keyColumn):string.Format("b_{0}", keyColumn);
            string a_ReferenceColumn = !isReferenceDB ? string.Format("a_{0}", referenceColumn):string.Format("b_{0}", referenceColumn);

            DataTable dt = new DataTable();
            dt.Columns.Add("Concept", typeof(String));
            dt.Columns.Add("PKColumnName", typeof(String));
            dt.Columns.Add("ColumnName", typeof(String));
            dt.Columns.Add("PKValue", typeof(String));
            dt.Columns.Add("Reference", typeof(String));
            dt.Columns.Add("ADHOC", typeof(String));
            dt.Columns.Add("UAT", typeof(String));


            foreach (DataRow dr in totalList.Rows)
            {
                if (matchedList.Rows.Cast<DataRow>().Any(x => x[a_keyColumn].ToString().Equals(dr[a_keyColumn].ToString(),StringComparison.OrdinalIgnoreCase))) continue;

                string referenceText = string.IsNullOrEmpty(referenceColumn)?"": dr[a_ReferenceColumn].ToString();

                DataRow drs = dt.NewRow();


                drs["Concept"] = concept;
                drs["PKColumnName"] = keyColumn;
                drs["ColumnName"] = isReferenceDB ? "Only in ReferenceDB" : "Only in DestionationDB";
                drs["PKValue"] = dr[a_keyColumn].ToString();
                drs["Reference"] = string.IsNullOrEmpty(referenceColumn) ? "" : referenceColumn;
                drs["ADHOC"] = referenceText;//.Replace("'", "''");
                drs["UAT"] = "";

                dt.Rows.Add(drs);
                //var result = new Result();
                //result.Concept = concept;
                //result.PKColumnName = keyColumn;
                //result.ColumnName = isReferenceDB ? "Only in before AU migration" : "Only in after AU migration";
                //result.PKValue = dr[a_keyColumn].ToString();
                //result.ReferenceName = string.IsNullOrEmpty(referenceColumn) ? "" : referenceColumn;
                //result.ReferenceDBValue = referenceText.Replace("'", "''");
               // DBHelper.LogActivity(GenerateLogScript(result));
            }

            //Save to Database

            BulkSaveDB(dt);
        }

        private static void GenerateMatchResult(DataTable result, string concept, string keyColumn, string referenceColumn)
        {
            string a_keyColumn = string.Format("a_{0}", keyColumn);
            string b_keyColumn = string.Format("b_{0}", keyColumn);
            string a_ReferenceColumn = string.Format("a_{0}", referenceColumn);
            string b_ReferenceColumn = string.Format("b_{0}", referenceColumn);

            DataTable dt = new DataTable();
            dt.Columns.Add("Concept", typeof(String));
            dt.Columns.Add("PKColumnName", typeof(String));
            dt.Columns.Add("ColumnName", typeof(String));
            dt.Columns.Add("PKValue", typeof(String));
            dt.Columns.Add("Reference", typeof(String));
            dt.Columns.Add("ADHOC", typeof(String));
            dt.Columns.Add("UAT", typeof(String));


            foreach (DataRow dr in result.Rows)
            {
                //if (!dr[a_keyColumn].ToString().Equals( "273121e0-b0c4-481d-ba26-a50f00a47b2c",StringComparison.OrdinalIgnoreCase)) continue;
                // 1 - 1
                // a_Id = b_Id --> PK1 then compare all the field

                if (  dr[a_keyColumn].ToString().Trim().Equals(dr[b_keyColumn].ToString().Trim(),StringComparison.OrdinalIgnoreCase) )
                {
                    foreach (var map in MappingList.Where(o => (o.IsPK != 1) && o.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase)))
                    {
                        DataRow mydr = dt.NewRow();

                        Result rs=  LogTheFieldDiff(concept, keyColumn, referenceColumn, a_keyColumn, a_ReferenceColumn, dr, map);

                        if (!string.IsNullOrEmpty(rs.Concept))
                        {
                            mydr["Concept"] = concept;
                            mydr["PKColumnName"] = keyColumn;
                            mydr["ColumnName"] = rs.ColumnName;
                            mydr["PKValue"] = rs.PKValue;
                            mydr["Reference"] = rs.ReferenceName;                    //string.IsNullOrEmpty(referenceColumn) ? "" : referenceColumn;
                            mydr["ADHOC"] = rs.ReferenceDBValue;
                            mydr["UAT"] = rs.DestinationDBValue;

                            dt.Rows.Add(mydr);
                        }

                    }
                }
                else
                {
                    // a_Id <> b_Id --> PK2 then i should skip the PK=2 field in the compare
                    foreach (var map in MappingList.Where(o => (o.IsPK != 1 && o.IsPK != 2) && o.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase)))
                    {
                        DataRow mydr = dt.NewRow();
                        Result rs = LogTheFieldDiff(concept, keyColumn, referenceColumn, a_keyColumn, a_ReferenceColumn, dr, map);

                        if (!string.IsNullOrEmpty(rs.Concept))
                        {
                            mydr["Concept"] = concept;
                            mydr["PKColumnName"] = keyColumn;
                            mydr["ColumnName"] = rs.ColumnName;
                            mydr["PKValue"] = rs.PKValue;
                            mydr["Reference"] = rs.ReferenceName;                    //string.IsNullOrEmpty(referenceColumn) ? "" : referenceColumn;
                            mydr["ADHOC"] = rs.ReferenceDBValue;
                            mydr["UAT"] = rs.DestinationDBValue;

                            dt.Rows.Add(mydr);
                        }
                        

                    }
                }
            }

            //Save to DB

            BulkSaveDB(dt);

        }

        private static void BulkSaveDB(DataTable dt)
        {
            using (SqlConnection _dconn = new SqlConnection(ConfigurationSettings.AppSettings["MappingDB_ConnectionString"]))
            {
                _dconn.Open();
                //Do a bulk copy for gaining better performance
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_dconn))
                {
                    //bulkCopy.ColumnMappings.Add("Concept", "Concept");
                    //bulkCopy.ColumnMappings.Add("PKColumnName", "PKColumnName");
                    //bulkCopy.ColumnMappings.Add("ColumnName", "ColumnName");
                    //bulkCopy.ColumnMappings.Add("PKValue", "PKValue");
                    //bulkCopy.ColumnMappings.Add("ADHOC", "ADHOC");
                    //bulkCopy.ColumnMappings.Add("UAT", "UAT");
                    //bulkCopy.ColumnMappings.Add("Reference", "Reference");

                    bulkCopy.BatchSize = 10000;
                    bulkCopy.BulkCopyTimeout = 1000;
                    bulkCopy.DestinationTableName = CompareTableResult;
                    bulkCopy.WriteToServer(dt.CreateDataReader());
                }
            }

        }



        private static Result LogTheFieldDiff(string concept, string keyColumn, string referenceColumn, string a_keyColumn, string a_ReferenceColumn, DataRow dr, Mapping map)
        {
            string a_column = string.Format("a_{0}", map.Field);
            string b_column = string.Format("b_{0}", map.Field);

            if (!dr[a_column].ToString().Trim().Equals(dr[b_column].ToString().Trim(), StringComparison.OrdinalIgnoreCase))
            {
                string referenceText = "";
                if (!string.IsNullOrEmpty(referenceColumn))
                {
                    referenceText = dr[a_ReferenceColumn].ToString();
                }

                //Log 
                var r = new Result();
                r.Concept = concept;
                r.PKColumnName = keyColumn;
                r.PKValue = dr[a_keyColumn].ToString();
                r.ReferenceName = referenceText;//.Replace("'", "''");
                r.ColumnName = map.Field;
                r.DestinationDBValue = dr[a_column].ToString();//.Replace("'", "''");
                r.ReferenceDBValue = dr[b_column].ToString();//.Replace("'", "''");

                return r;
                //mydr["Concept"] = concept;
                //mydr["PKColumnName"] = keyColumn;
                //mydr["PKValue"] = dr[a_keyColumn].ToString();
                //mydr["Reference"] = referenceText.Replace("'", "''");
                //mydr["ColumnName"] = map.Field;
                //mydr["ADHOC"] = dr[b_column].ToString();//.Replace("'", "''");
                //mydr["UAT"] = dr[a_column].ToString();//.Replace("'", "''");

                //DBHelper.LogActivity(GenerateLogScript(r));
            }
            else
                return new Result();
        }


         private static DataTable GenerateMatchRecords(string concept, string keycolumn, string compositekey)
        {
            string fields = "";
            List<string> sb = new List<string>();

            foreach (var field in MappingList.Where(o => o.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase)))
            {
                //a.{0} a_{0}, b.{0} b_{0}
                sb.Add(string.Format("ISNULL( CAST( a.{0} as NVARCHAR(max)),'') a_{0}, ISNULL( CAST(b.{0} as NVARCHAR(max)),'') b_{0}", field.Field));
            }

            fields = string.Join(",", sb.ToArray());


            string matchSql = string.Format(@"
SELECT 
	{0}
FROM {1}.dbo.{3} a
INNER JOIN {2}.dbo.{3} b ON a.{4} = b.{4}
{6}union SELECT 	{0}
{6}FROM {1}.dbo.{3} a
{6}INNER JOIN {2}.dbo.{3} b ON {5}
{6}WHERE NOT EXISTS (
{6}	SELECT * FROM {1}.dbo.{3} a1
{6}	INNER JOIN {2}.dbo.{3} b1 ON a1.{4} = b1.{4}
{6}	WHERE a.{4} = a1.{4}
{6} )
{6} AND NOT EXISTS (
{6}	SELECT * FROM {1}.dbo.{3} a1
{6}	INNER JOIN {2}.dbo.{3} b1 ON a1.{4} = b1.{4}
{6}	WHERE b.{4} = a1.{4}
{6})
", fields, TestDBName, ReferenceDBName, concept, keycolumn, compositekey,string.IsNullOrEmpty(compositekey)?"--":""
 );
            DataTable result = DBHelper.GetReferenceDBDataTable(matchSql);

            return result;
        }


         private static DataTable GenerateRecords(string concept,string dbName,bool IsReferenceDB)
         {
             string fields = "";
             List<string> sb = new List<string>();

             foreach (var field in MappingList.Where(o => o.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase)))
             {
                 //a.{0} a_{0}, b.{0} b_{0}
                 sb.Add(string.Format("ISNULL( CAST( {0}.{1} as NVARCHAR(max)),'') {0}_{1}", !IsReferenceDB?"a":"b", field.Field));
             }

             fields = string.Join(",", sb.ToArray());


             string matchSql = string.Format(@"
SELECT 
	{0}
FROM {1}.dbo.{2} {3}

", fields, dbName, concept, !IsReferenceDB ? "a" : "b"
  );
             DataTable result = DBHelper.GetReferenceDBDataTable(matchSql);

             return result;
         }




        private static void LogMatchedCount(string concept, int matchedCount)
        {
            var matchedByPK = new Result();
            matchedByPK.Concept = concept;

            if (matchedCount > 0)
            {
                matchedByPK.DestinationDBValue = "Total matched records by PK is :" + matchedCount.ToString();
                matchedByPK.ReferenceDBValue = "Total matched records by PK is :" + matchedCount.ToString();
            }
            else
            {
                matchedByPK.DestinationDBValue = "No matched records by Key";
                matchedByPK.ReferenceDBValue = "No matched records by Key";
            }

            DBHelper.LogActivity(GenerateLogScript(matchedByPK));
        }

        #endregion

       
        #region Helper
        //public string GetSQL(string type)
        //{
        //    string sql = "";

        //    switch (type)
        //    {
        //        case "PK1Compare":
        //            sql = @"select a.{0} from {1}.dbo.{2} a inner join {3}.dbo.{2} b on a.{0}= b.{0}";
        //            break;
        //        case "PK2Compare":
        //            sql = 
        //    }

        //    return sql;
        //}

        private static string GetRetrieveScript(string concept, string columnname)
        {
            return @"select " + columnname + " From " + concept;
        }
       
        private static string GetcompositeKey(List<Mapping> mapping, string concept)
        {
            //Expectation: or (a.Name = b.Name and a.Description = b.Description)
            var compositeKeyList = mapping.Where(x => x.Concept == concept && x.IsPK == 2);

            string compositeKey = "";
            int counter = 0;
            string condition = "";
            foreach (var key in compositeKeyList)
            {
                if (counter == 0) condition = " or (";
                else condition = "and";

                compositeKey += string.Format(" {1} a.{0}= b.{0}", key.Field, condition);

                counter++;
            }

            if (!string.IsNullOrEmpty(compositeKey))
                compositeKey += ")";

            return compositeKey;
        }

        public static bool CheckPK2(string pk, string connectionString, string concept)
        {
            if (string.IsNullOrEmpty(ConstructCompositeKeyGrouping(concept))) return true;


            string sql = string.Format(@"SELECT {2}
                 FROM  {1} {1}
                 WHERE {1}.{0} NOT IN (
	                SELECT a.{0} from {1} a
	                GROUP BY a.{0}
	                HAVING COUNT(*) = 1
                 )
                 GROUP BY {2}
                 HAVING COUNT(*) > 1", pk, concept, ConstructCompositeKeyColumn(concept));
            //select {0} from {1} a group by {0} having count(*) > 1"


            //Run the script in both Before and After Interim DB version. If either 1 return >1, print the exception and return false. Else true

            var destination = DBHelper.GetDestinationDBDataTableCount(sql);
            var reference = DBHelper.GetReferenceDBDataTableCount(sql);

            if (destination > 0 || reference > 0)
            {
                log.Info("Duplicate records for PrimaryKey:" + ConstructCompositeKeyGrouping(concept) + " table :" + concept);
                return false;
            }

            return true;
        }

        private static string ConstructCompositeKeyGrouping(string concept)
        {
            //Expectation: or (a.Name = b.Name and a.Description = b.Description)
            var compositeKeyList = MappingList.Where(x => x.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase) && x.IsPK == 2);

            string compositeKey = "";
            int counter = 0;
            string condition = "";
            foreach (var key in compositeKeyList)
            {
                if (counter == 0) condition = "";
                else condition = " and ";

                compositeKey += string.Format("{1} ISNULL(a.{0},'') =ISNULL(b.{0},'')  ", key.Field, condition);

                counter++;
            }

            return compositeKey;
        }

        private static string ConstructCompositeKeyField(string concept)
        {
            //Expectation: or (a.Name = b.Name and a.Description = b.Description)
            var compositeKeyList = MappingList.Where(x => x.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase) && x.IsPK == 2).ToList();

            //string compositeKey = "";

            string compositeKey = string.Join("+'|'+", compositeKeyList.Select(o => "CAST(a." + o.Field + " AS  NVARCHAR(200))"));

            //foreach (var key in compositeKeyList)
            //{
            //    compositeKey += "a."+key.Field+"|";
            //}

            //return compositeKey.TrimEnd('|');
            return compositeKey;
        }

        private static string ConstructCompositeKeyColumn(string concept)
        {
            var compositeKeyList = MappingList.Where(x => x.Concept.Equals(concept, StringComparison.OrdinalIgnoreCase) && x.IsPK == 2);

            string compositeKey = "";
            int counter = 0;
            string condition = "";
            foreach (var key in compositeKeyList)
            {
                if (counter == 0) condition = "";
                else condition = " , ";

                compositeKey += string.Format("{1}{2}.{0}", key.Field, condition, concept);

                counter++;
            }

            return compositeKey;
        }
        #endregion

        #region Log
        private static void LogTableCount(string concept, string pkcolumn)
        {

            var destionationcount = DBHelper.GetTableCount(GetRetrieveScript(concept, pkcolumn), false, true);
            var referencecount = DBHelper.GetTableCount(GetRetrieveScript(concept, pkcolumn), true, false);
            Result logresult = new Result();
            logresult.Concept = concept;
            logresult.DestinationDBValue = "Table Count is:" + destionationcount.ToString();
            logresult.ReferenceDBValue = "Table count is:" + referencecount.ToString();
            log.Info("Log Table Level Info");
            DBHelper.LogActivity(GenerateLogScript(logresult));

        }

        private static string GenerateLogScript(Result result)
        {
            return string.Format(@"insert into [" + CompareTableResult + "] (Concept,PKColumnName,PKValue,ColumnName,ADHOC,UAT,Reference) values"
                  + " (N'{0}',N'{1}',N'{2}',N'{3}',N'{4}',N'{5}',N'{6}')",
                  result.Concept, result.PKColumnName, result.PKValue, result.ColumnName, result.ReferenceDBValue, result.DestinationDBValue, result.ReferenceName);
        }

        #endregion
    }
}
