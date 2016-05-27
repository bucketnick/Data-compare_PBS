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

namespace DBComparer
{
     public static class Application
    {
         private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

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
                     string.Format(@"select a.{0} from {1}.dbo.{2} a inner join [CMSINTERIMDB].{3}.dbo.{2} b
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
                     string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        cdList = DBHelper.GetCompareDictionaryList(retrievePKValue_ReferenceDBValue_DestinationDBValue);

                    }

                    else
                    {
                        string retrievePKValue_ReferenceDBValue_DestinationDBValue_BriefMonograph =
                        string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} where a.MonographTypeCT='58ecbf9d-c318-496a-98ec-a32100c7f139' and b.MonographTypeCT= '8e76b248-9f0f-4474-91f3-2bdc87c2a5c7' ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        cdList = DBHelper.GetCompareDictionaryList(retrievePKValue_ReferenceDBValue_DestinationDBValue_BriefMonograph);


                        string retrievePKValue_ReferenceDBValue_DestinationDBValue_FullMonograph =
                        string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} where a.MonographTypeCT='8b29f44d-26c7-47c6-8a65-a32100c8190d' and b.MonographTypeCT= '3ea96813-3bae-464a-a1ed-be3736589701' ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        List<CompareDictionary> cdList_FullMonograph = DBHelper.GetCompareDictionaryList(retrievePKValue_ReferenceDBValue_DestinationDBValue_FullMonograph);

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
                 string.Format(@"select a.{0},a.{1}, b.{5} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", spk, sdestinationcolumnname, sconcept, TocompareDBname, ReferenceDBname, sreferencecolumnname);
                List<CompareDictionary> scdList = DBHelper.GetCompareDictionaryList(sretrievePKValue_ReferenceDBValue_DestinationDBValue);
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
 " inner join  (select legacyId, Address1,Address2,Address3,PostalCode  from [CMSINTERIMDB].InterimDB_AU_20150121_120031_Release.dbo.address ) adhoc "
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

                    //inner join   [CMSINTERIMDB].InterimDB_AU_20150121_120031_Release.dbo.Contact b

                    //on  c.LegacyId= b.legacyid and c.ContactType= b.contacttype

                    //where c.Data<> b.data

                }
            }

            #endregion

        }
        public static void AsiaDBcomparer()
        {

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
                     string.Format(@"select a.{0} from {1}.dbo.{2} a inner join [CMSINTERIMDB].{3}.dbo.{2} b
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
                        string retrievePKValue_ReferenceDBValue_DestinationDBValue =
                     string.Format(@"select a.{0},a.{1}, b.{1} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", pkcolunmn, columnname, concept, TocompareDBname, ReferenceDBname);
                        cdList = DBHelper.GetCompareDictionaryList(retrievePKValue_ReferenceDBValue_DestinationDBValue);

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
                 string.Format(@"select a.{0},a.{1}, b.{5} from {3}.dbo.{2} a inner join [CMSINTERIMDB].{4}.dbo.{2} b
                        on a.{0}= b.{0} ", spk, sdestinationcolumnname, sconcept, TocompareDBname, ReferenceDBname, sreferencecolumnname);
                List<CompareDictionary> scdList = DBHelper.GetCompareDictionaryList(sretrievePKValue_ReferenceDBValue_DestinationDBValue);
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





        
        }
    }
}
