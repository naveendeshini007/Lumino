#######################Reviewed code 


import sys
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name, aggregate
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info('Logger enabled and integrated with CloudWatch Logs')

# Initialize AWS Glue job
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.shuffle.partitions", "100")
    spark.conf.set("spark.default.parallelism", "100")
    logger.info(f"Job {args['JOB_NAME']} initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Glue job: {str(e)}")
    raise

# Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# base_input_path = "s3://lumino-data/xml_extraction_test1/xml_raw/"
base_input_path = "s3://lumino-data/xml_data_extraction/xml_raw/"


# years = ["2019", "2020"]
# years = ["2020"]
years = ["2023"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }

subfolders = {
    # "2019": ["download990xml_2019_1", "2019_TEOS_XML_CT1", "download990xml_2019_2"]
    # "2019": ["2019_TEOS_XML_CT1", "download990xml_2019_4", "download990xml_2019_5", "download990xml_2019_6", "download990xml_2019_7", "download990xml_2019_8"]
    # "2020": ["download990xml_2020_8"]
    # "2020": ["2020_download990xml_2020_1", "2020_download990xml_2020_5", "2020_TEOS_XML_CT1", "download990xml_2020_2", "download990xml_2020_3"]
    # "2020": ["download990xml_2020_4", "download990xml_2020_6", "download990xml_2020_7"]   this is not yet done
    # "2023": ["2023_TEOS_XML_01A"]
    "2023": ["2023_TEOS_XML_02A"]
    
}

#   2019_TEOS_XML_CT1, download990xml_2019_1, download990xml_2019_2/, download990xml_2019_3/, download990xml_2019_4/, download990xml_2019_5/, download990xml_2019_6/, download990xml_2019_7/, download990xml_2019_8/

output_base_path = "s3://lumino-data/xml_data_extraction/xml_processed_data/"
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract_3/"
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract_2/"
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"


# Initialize S3 client to check for files
s3_client = boto3.client('s3')
bucket_name = "lumino-data"

# Define schemas
return_header_schema = StructType([
    StructField("ReturnTypeCd", StringType(), True),
    StructField("TaxPeriodBeginDt", StringType(), True),
    StructField("TaxPeriodEndDt", StringType(), True),
    StructField("TaxYr", StringType(), True),
    StructField("Filer", StructType([
        StructField("EIN", StringType(), True),
        StructField("BusinessName", StructType([
            StructField("BusinessNameLine1Txt", StringType(), True),
            StructField("BusinessNameLine2Txt", StringType(), True)
        ]), True),
        StructField("BusinessNameControlTxt", StringType(), True),
        StructField("PhoneNum", StringType(), True),
        StructField("ForeignPhoneNum", StringType(), True),
        StructField("USAddress", StructType([
            StructField("AddressLine1Txt", StringType(), True),
            StructField("AddressLine2Txt", StringType(), True),
            StructField("CityNm", StringType(), True),
            StructField("StateAbbreviationCd", StringType(), True),
            StructField("ZIPCd", StringType(), True)
        ]), True)
    ]), True)
])

board_member_990_schema = StructType([
    StructField("PersonNm", StringType(), True),
    StructField("BusinessName", StructType([
        StructField("BusinessNameLine1Txt", StringType(), True),
        StructField("BusinessNameLine2Txt", StringType(), True)
    ]), True),
    StructField("TitleTxt", StringType(), True),
    StructField("AverageHoursPerWeekRt", DoubleType(), True),
    StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
    StructField("ReportableCompFromOrgAmt", DoubleType(), True),
    StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
    StructField("OtherCompensationAmt", DoubleType(), True),
    StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
    StructField("OfficerInd", StringType(), True),
    StructField("HighestCompensatedEmployeeInd", StringType(), True),
    StructField("KeyEmployeeInd", StringType(), True),
    StructField("FormerInd", StringType(), True)
])

board_member_schema = StructType([
    StructField("PersonNm", StringType(), True),
    StructField("BusinessName", StructType([
        StructField("BusinessNameLine1Txt", StringType(), True),
        StructField("BusinessNameLine2Txt", StringType(), True)
    ]), True),
    StructField("TitleTxt", StringType(), True),
    StructField("USAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
    StructField("AverageHoursPerWeekRt", DoubleType(), True),
    StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
    StructField("CompensationAmt", DoubleType(), True),
    StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
    StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
    StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
    StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
    StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
    StructField("ContractorPaidOver50kCnt", StringType(), True),
    StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
    StructField("OfficerInd", StringType(), True),
    StructField("HighestCompensatedEmployeeInd", StringType(), True),
    StructField("ReportableCompFromOrgAmt", DoubleType(), True),
    StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
    StructField("OtherCompensationAmt", DoubleType(), True)
])

employee_schema = StructType([
    StructField("PersonNm", StringType(), True),
    StructField("USAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("TitleTxt", StringType(), True),
    StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
    StructField("CompensationAmt", DoubleType(), True),
    StructField("EmployeeBenefitsAmt", DoubleType(), True),
    StructField("ExpenseAccountAmt", DoubleType(), True)
])

grantee_schema = StructType([
    StructField("RecipientPersonNm", StringType(), True),
    StructField("RecipientName", StringType(), True),
    StructField("RecipientBusinessName", StructType([
        StructField("BusinessNameLine1Txt", StringType(), True),
        StructField("BusinessNameLine2Txt", StringType(), True)
    ]), True),
    StructField("RecipientUSAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("USAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("RecipientFoundationStatusTxt", StringType(), True),
    StructField("GrantOrContributionPurposeTxt", StringType(), True),
    StructField("PurposeOfGrantTxt", StringType(), True),
    StructField("Amt", DoubleType(), True),
    StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
    StructField("OnlyContriToPreselectedInd", StringType(), True),
    StructField("RecipientRelationshipTxt", StringType(), True),
    StructField("RecipientEIN", StringType(), True),
    StructField("IRCSectionDesc", StringType(), True),
    StructField("CashGrantAmt", DoubleType(), True),
    StructField("TaxYr", StringType(), True)
])

contributor_schema = StructType([
    StructField("ContributorPersonNm", StringType(), True),
    StructField("ContributorBusinessName", StructType([
        StructField("BusinessNameLine1Txt", StringType(), True),
        StructField("BusinessNameLine2Txt", StringType(), True)
    ]), True),
    StructField("ContributorUSAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("TotalContributionsAmt", DoubleType(), True),
    StructField("PersonContributionInd", StringType(), True),
    StructField("PayrollContributionInd", StringType(), True),
    StructField("NoncashContributionInd", StringType(), True)
])

contact_schema = StructType([
    StructField("RecipientPersonNm", StringType(), True),
    StructField("RecipientUSAddress", StructType([
        StructField("AddressLine1Txt", StringType(), True),
        StructField("AddressLine2Txt", StringType(), True),
        StructField("CityNm", StringType(), True),
        StructField("StateAbbreviationCd", StringType(), True),
        StructField("ZIPCd", StringType(), True)
    ]), True),
    StructField("RecipientPhoneNum", StringType(), True),
    StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
    StructField("SubmissionDeadlinesTxt", StringType(), True),
    StructField("RestrictionsOnAwardsTxt", StringType(), True)
])

grants_other_assist_schema = StructType([
    StructField("GrantTypeTxt", StringType(), True),
    StructField("RecipientCnt", StringType(), True),
    StructField("CashGrantAmt", DoubleType(), True)
])

form_990_grantee_schema = StructType([
    StructField("ReturnHeader", StructType([
        StructField("ReturnTypeCd", StringType(), True),
        StructField("TaxYr", StringType(), True),
        StructField("TaxPeriodBeginDt", StringType(), True),
        StructField("TaxPeriodEndDt", StringType(), True),
        StructField("Filer", StructType([
            StructField("EIN", StringType(), True),
            StructField("BusinessName", StructType([
                StructField("BusinessNameLine1Txt", StringType(), True),
                StructField("BusinessNameLine2Txt", StringType(), True)
            ]), True),
            StructField("BusinessNameControlTxt", StringType(), True),
            StructField("PhoneNum", StringType(), True),
            StructField("ForeignPhoneNum", StringType(), True),
            StructField("USAddress", StructType([
                StructField("AddressLine1Txt", StringType(), True),
                StructField("AddressLine2Txt", StringType(), True),
                StructField("CityNm", StringType(), True),
                StructField("StateAbbreviationCd", StringType(), True),
                StructField("ZIPCd", StringType(), True)
            ]), True)
        ]), True)
    ]), True),
    StructField("ReturnData", StructType([
        StructField("IRS990ScheduleI", StructType([
            StructField("RecipientTable", ArrayType(grantee_schema), True),
            StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
            StructField("Total501c3OrgCnt", StringType(), True),
            StructField("TotalOtherOrgCnt", IntegerType(), True),
            StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
                StructField("GrantTypeTxt", StringType(), True),
                StructField("RecipientCnt", StringType(), True),
                StructField("CashGrantAmt", DoubleType(), True)
            ]), True)
        ]), True),
        StructField("IRS990", StructType([
            StructField("GrantsToDomesticOrgsGrp", StructType([
                StructField("TotalAmt", DoubleType(), True)
            ]), True),
            StructField("TotalFunctionalExpensesGrp", StructType([
                StructField("TotalAmt", DoubleType(), True),
                StructField("ProgramServicesAmt", DoubleType(), True),
                StructField("ManagementAndGeneralAmt", DoubleType(), True),
                StructField("FundraisingAmt", DoubleType(), True)
            ]), True),
            StructField("TotalVolunteersCnt", StringType(), True),
            StructField("ActivityOrMissionDesc", StringType(), True),
            StructField("FederatedCampaignsAmt", DoubleType(), True),
            StructField("MembershipDuesAmt", DoubleType(), True),
            StructField("FundraisingAmt", DoubleType(), True),
            StructField("RelatedOrganizationsAmt", DoubleType(), True),
            StructField("GovernmentGrantsAmt", DoubleType(), True),
            StructField("AllOtherContributionsAmt", DoubleType(), True),
            StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
            StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
            StructField("TotalOtherCompensationAmt", DoubleType(), True),
            StructField("WebsiteAddressTxt", StringType(), True),
            StructField("NoncashContributionsAmt", DoubleType(), True),  # Added
            StructField("CYProgramServiceRevenueAmt", DoubleType(), True),  # Added
            StructField("CYInvestmentIncomeAmt", DoubleType(), True),  # Added
            StructField("CYOtherRevenueAmt", DoubleType(), True),  # Added
            StructField("CYTotalRevenueAmt", DoubleType(), True),  # Added
            StructField("CYGrantsAndSimilarPaidAmt", DoubleType(), True),  # Added
            StructField("CYTotalExpensesAmt", DoubleType(), True)  # Added
        ]), True)
    ]), True)
])

unified_schema = StructType([
    StructField("ReturnHeader", return_header_schema, True),
    StructField("ReturnData", StructType([
        StructField("IRS990PF", StructType([
            StructField("AnalysisOfRevenueAndExpenses", StructType([
                StructField("TotOprExpensesRevAndExpnssAmt", DoubleType(), True),
                StructField("InterestOnSavRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("DividendsRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("NetRentalIncomeOrLossAmt", DoubleType(), True),  # Added
                StructField("NetGainSaleAstRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("GrossProfitRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("OtherIncomeRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("TotalRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("ContriPaidRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("TotalExpensesRevAndExpnssAmt", DoubleType(), True),  # Added
                StructField("ContriRcvdRevAndExpnssAmt", DoubleType(), True)  # Added for other_contributions
            ]), True),
            StructField("AnalysisIncomeProducingActyGrp", StructType([
                StructField("SubtotalsIncmProducingActyGrp", StructType([
                    StructField("UnrelatedBusinessTaxblIncmAmt", DoubleType(), True),  # Added
                    StructField("ExclusionAmt", DoubleType(), True),  # Added
                    StructField("RelatedOrExemptFunctionIncmAmt", DoubleType(), True)  # Added
                ]), True),
                StructField("ProgramServiceRevenueDtl", ArrayType(StructType([
                    StructField("UnrelatedBusinessTaxblIncmAmt", DoubleType(), True),
                    StructField("ExclusionAmt", DoubleType(), True),
                    StructField("RelatedOrExemptFunctionIncmAmt", DoubleType(), True)
                ])), True),
                StructField("MembershipDuesAndAssmntGrp", StructType([
                    StructField("UnrelatedBusinessTaxblIncmAmt", DoubleType(), True),  # Added
                    StructField("ExclusionAmt", DoubleType(), True),  # Added
                    StructField("RelatedOrExemptFunctionIncmAmt", DoubleType(), True)  # Added
                ]), True)
            ]), True),
            StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
                StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
                StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
                StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
                StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
                StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
                StructField("ContractorPaidOver50kCnt", StringType(), True)
            ]), True),
            StructField("SupplementaryInformationGrp", StructType([
                StructField("ContributingManagerNm", StringType(), True),
                StructField("ShareholderManagerNm", StringType(), True),
                StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
                StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
                StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
            ]), True),
            StructField("StatementsRegardingActyGrp", StructType([
                StructField("WebsiteAddressTxt", StringType(), True)
            ]), True)
        ]), True),
        StructField("IRS990ScheduleB", StructType([
            StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
        ]), True),
        StructField("IRS990", StructType([
            StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
            StructField("GrantsToDomesticOrgsGrp", StructType([
                StructField("TotalAmt", DoubleType(), True)
            ]), True),
            StructField("TotalFunctionalExpensesGrp", StructType([
                StructField("TotalAmt", DoubleType(), True),
                StructField("ProgramServicesAmt", DoubleType(), True),
                StructField("ManagementAndGeneralAmt", DoubleType(), True),
                StructField("FundraisingAmt", DoubleType(), True)
            ]), True),
            StructField("TotalVolunteersCnt", StringType(), True),
            StructField("ActivityOrMissionDesc", StringType(), True),
            StructField("FederatedCampaignsAmt", DoubleType(), True),
            StructField("MembershipDuesAmt", DoubleType(), True),
            StructField("FundraisingAmt", DoubleType(), True),
            StructField("RelatedOrganizationsAmt", DoubleType(), True),
            StructField("GovernmentGrantsAmt", DoubleType(), True),
            StructField("AllOtherContributionsAmt", DoubleType(), True),
            StructField("WebsiteAddressTxt", StringType(), True),
            StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
            StructField("TotalOtherCompensationAmt", DoubleType(), True),
            StructField("NoncashContributionsAmt", DoubleType(), True),  # Added
            StructField("CYProgramServiceRevenueAmt", DoubleType(), True),  # Added
            StructField("CYInvestmentIncomeAmt", DoubleType(), True),  # Added
            StructField("CYOtherRevenueAmt", DoubleType(), True),  # Added
            StructField("CYTotalRevenueAmt", DoubleType(), True),  # Added
            StructField("CYGrantsAndSimilarPaidAmt", DoubleType(), True),  # Added
            StructField("CYTotalExpensesAmt", DoubleType(), True)  # Added
        ]), True),
        StructField("IRS990ScheduleI", StructType([
            StructField("RecipientTable", ArrayType(grantee_schema), True),
            StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
            StructField("Total501c3OrgCnt", StringType(), True),
            StructField("TotalOtherOrgCnt", IntegerType(), True)
        ]), True),
        StructField("IRS990EZ", StructType([
            StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
            StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
            StructField("SalariesOtherCompEmplBnftAmt", DoubleType(), True),
            StructField("FeesAndOtherPymtToIndCntrctAmt", DoubleType(), True),
            StructField("OccupancyRentUtltsAndMaintAmt", DoubleType(), True),
            StructField("PrintingPublicationsPostageAmt", DoubleType(), True),
            StructField("OtherExpensesTotalAmt", DoubleType(), True),
            StructField("PrimaryExemptPurposeTxt", StringType(), True),
            StructField("WebsiteAddressTxt", StringType(), True),
            StructField("ProgramServiceRevenueAmt", DoubleType(), True),  # Added
            StructField("InvestmentIncomeAmt", DoubleType(), True),  # Added
            StructField("OtherRevenueTotalAmt", DoubleType(), True),  # Added
            StructField("TotalRevenueAmt", DoubleType(), True),  # Added
            StructField("TotalExpensesAmt", DoubleType(), True),  # Added
            StructField("MembershipDuesAmt", DoubleType(), True),  # Added
            StructField("FundraisingGrossIncomeAmt", DoubleType(), True), # Added
            StructField("ContributionsGiftsGrantsEtcAmt", DoubleType(), True) # Added
        ]), True)
    ]), True)
])

for year in years:
    for subfolder in subfolders[year]:
        input_path = f"{base_input_path}{year}/{subfolder}/"
        s3_prefix = f"xml_data_extraction/xml_raw/{year}/{subfolder}/"
        logger.info(f"Currently processing folder: s3://{bucket_name}/{s3_prefix}")
        
        # Check if files exist in the S3 path
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
            if 'Contents' not in response or len(response['Contents']) == 0:
                logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
                continue
        except s3_client.exceptions.ClientError as e:
            logger.error(f"Error listing files in s3://{bucket_name}/{s3_prefix}: {str(e)}")
            continue

        try:
            # Initialize empty DataFrames
            board_member_990_df = None
            grantee_990_df = None

            # Read Form 990 for board members and grantees
            form_990_df = spark.read \
                .format("xml") \
                .option("rowTag", "Return") \
                .option("wholeFile", "true") \
                .option("inferSchema", "false") \
                .option("ignoreNamespace", "true") \
                .schema(form_990_grantee_schema) \
                .load(f"{input_path}*.xml") \
                .withColumn("input_file_name", input_file_name()) \
                .filter((col("ReturnHeader.TaxYr").isNull() | (col("ReturnHeader.TaxYr") >= "2019")) & col("ReturnHeader.ReturnTypeCd").isin("990"))
                
            # .filter(col("ReturnHeader.ReturnTypeCd") == "990")
                
            
            # Extract object_id
            form_990_df = form_990_df.withColumn(
                "filename",
                substring_index(col("input_file_name"), "/", -1)
            ).withColumn(
                "object_id",
                regexp_extract(col("filename"), r"(\d+)", 1)
            ).drop("filename")
            
            # Log row count
            form_990_count = form_990_df.count()
            logger.info(f"Form 990 rows: {form_990_count}")
            if form_990_count == 0:
                logger.warning(f"No Form 990 data >= 2019 in {input_path}")
            else:
                # Form 990 Board Member DataFrame
                board_member_990_df = form_990_df.select(
                    col("object_id"),
                    col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                    col("ReturnHeader.TaxYr").alias("tax_year"),
                    col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                    explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
                    # col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
                    # col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
                    # lit(None).cast(StringType()).alias("parent_highest_compensation"),
                    # lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
                    # lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
                    # lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
                ).select(
                    col("object_id"),
                    col("grantor_ein"),
                    col("tax_year"),
                    col("return_type_cd"),
                    # col("board_member.PersonNm").alias("board_member_person_name"),
                    coalesce(
                        col("board_member.PersonNm"),
                        concat_ws(" ",
                            col("board_member.BusinessName.BusinessNameLine1Txt"),
                            col("board_member.BusinessName.BusinessNameLine2Txt")
                        )
                    ).alias("board_member_person_name"),
                    col("board_member.TitleTxt").alias("board_member_title"),
                    lit(None).cast(StringType()).alias("board_member_address"),
                    lit(None).cast(StringType()).alias("board_member_city"),
                    lit(None).cast(StringType()).alias("board_member_state_cd"),
                    lit(None).cast(StringType()).alias("board_member_zip_code"),
                    col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
                    col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
                    lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
                    col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
                    lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
                    col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
                    lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
                    col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
                    # lit(None).cast(StringType()).alias("board_member_highest_compensation"),
                    # lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
                    # lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
                    # lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
                    col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
                    col("board_member.OfficerInd").alias("board_member_officer_ind"),
                    col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind")
                    # col("board_member_total_reportable_comp_org_amt"),
                    # col("board_member_total_other_compensation_amt")
                )
                
                # Log board member row count
                count_990 = board_member_990_df.count()
                logger.info(f"Form 990 Board Member rows: {count_990}")
                if count_990 == 0:
                    logger.warning(f"No Form 990 board member data for {input_path}")

                # Form 990 Grantee DataFrame
                grantee_990_df = form_990_df.select(
                    col("object_id"),
                    col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                    col("ReturnHeader.TaxYr").alias("tax_year"),
                    col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                    col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
                    col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
                    when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
                         (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
                         concat_ws(" ",
                             col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
                             col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
                         )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
                    col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
                    explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
                    # col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
                    # col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
                    # col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
                    # col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
                    # col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other")
                    # col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
                    # col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
                    # col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
                    # col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
                    # col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
                    # col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
                    # col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
                    # col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
                ).select(
                    col("object_id"),
                    col("grantor_ein"),
                    col("tax_year"),
                    col("return_type_cd"),
                    col("grantor_business_name"),
                    col("tax_period_begin_dt"),
                    col("tax_period_end_dt"),
                    col("grantor_business_name_control"),
                    coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
                    when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
                         (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
                         when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
                              (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
                              concat_ws(" ",
                                  col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
                                  col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
                              ))
                         .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
                    .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
                    when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
                         concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
                    .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
                    col("grantee.USAddress.CityNm").alias("grantee_city"),
                    col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
                    col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
                    col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
                    col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
                    coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt")).alias("grantee_amount"),
                    col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
                    col("grantee.RecipientEIN").alias("grantee_ein"),
                    col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
                    # col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
                    lit(None).cast(StringType()).alias("grant_paid_year"),
                    # col("grantee_501c3_org_count"),
                    # col("grantee_other_org_count"),
                    # col("grantee_individual_grant_type"),
                    # col("grantee_individual_recipient_count"),
                    # col("grantee_individual_cash_amount_other"),
                    # col("total_volunteers"),
                    # col("mission_statement"),
                    # col("federal_campaign_contributions"),
                    # col("membership_dues"),
                    # col("fundraising_contributions"),
                    # col("related_organization_support"),
                    # col("government_grants"),
                    # col("other_contributions"),
                    # lit(None).cast(StringType()).alias("primary_purpose_statement"),
                    lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
                    lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
                    lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
                    lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
                    lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
                )
                
                # Log grantee row count
                grantee_990_count = grantee_990_df.count()
                logger.info(f"Form 990 Grantee rows: {grantee_990_count}")
                if grantee_990_count == 0:
                    logger.warning(f"No Form 990 grantee data for {input_path}")

            # Read with unified schema for 990EZ and 990PF
            df = spark.read \
                .format("xml") \
                .option("rowTag", "Return") \
                .option("wholeFile", "true") \
                .option("inferSchema", "false") \
                .option("ignoreNamespace", "true") \
                .schema(unified_schema) \
                .load(f"{input_path}*.xml") \
                .withColumn("input_file_name", input_file_name()) \
                .filter((col("ReturnHeader.TaxYr").isNull() | (col("ReturnHeader.TaxYr") >= "2019")) & col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF")) \
                .cache()
                
            
            # Extract object_id
            df = df.withColumn(
                "filename",
                substring_index(col("input_file_name"), "/", -1)
            ).withColumn(
                "object_id",
                regexp_extract(col("filename"), r"(\d+)", 1)
            ).drop("filename")
            
            if not df.take(1):
                logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
                continue

            # df2 = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))
            
            # df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF"))
            
            
            df2 = df
               
            df = df.filter((col("ReturnHeader.TaxYr").isNull() | (col("ReturnHeader.TaxYr") >= "2019")) & 
               col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF"))

            # Form 990EZ Board Member DataFrame
            board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member")
                # lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
                # lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
                # lit(None).cast(StringType()).alias("parent_highest_compensation"),
                # lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
                # lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
                # lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                # col("board_member.PersonNm").alias("board_member_person_name"),
                coalesce(
                    col("board_member.PersonNm"),
                    concat_ws(" ",
                        col("board_member.BusinessName.BusinessNameLine1Txt"),
                        col("board_member.BusinessName.BusinessNameLine2Txt")
                    )
                ).alias("board_member_person_name"),
                col("board_member.TitleTxt").alias("board_member_title"),
                when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
                .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
                col("board_member.USAddress.CityNm").alias("board_member_city"),
                col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
                col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
                col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
                # col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
                # col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
                lit(None).cast(DoubleType()).alias("board_member_average_hours_related_org"),
                lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
                col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
                col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
                lit(None).cast(DoubleType()).alias("board_member_reportable_comp_related_org_amt"),
                lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
                # col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
                # col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
                col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
                # col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
                # col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
                # col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
                # col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
                col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
                col("board_member.OfficerInd").alias("board_member_officer_ind"),
                col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
                # col("board_member_total_reportable_comp_org_amt"),
                # col("board_member_total_other_compensation_amt")
            )

            # Form 990PF Board Member DataFrame
            board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
                # lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
                # lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
                # col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
                # col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
                # col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
                # col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                # col("board_member.PersonNm").alias("board_member_person_name"),
                # coalesce(
                #     col("board_member.PersonNm"),
                #     col("board_member.BusinessName.BusinessNameLine1Txt")
                # ).alias("board_member_person_name"),
                coalesce(
                    col("board_member.PersonNm"),
                    concat_ws(" ",
                        col("board_member.BusinessName.BusinessNameLine1Txt"),
                        col("board_member.BusinessName.BusinessNameLine2Txt")
                    )
                ).alias("board_member_person_name"),
                col("board_member.TitleTxt").alias("board_member_title"),
                when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
                .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
                col("board_member.USAddress.CityNm").alias("board_member_city"),
                col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
                col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
                col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
                # col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
                lit(None).cast(DoubleType()).alias("board_member_average_hours_related_org"),
                col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
                # col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
                lit(None).cast(DoubleType()).alias("board_member_reportable_comp_org_amt"),
                col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
                # col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
                lit(None).cast(DoubleType()).alias("board_member_reportable_comp_related_org_amt"),
                col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
                # col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
                lit(None).cast(DoubleType()).alias("board_member_estimated_other_compensation_amt"),
                # col("parent_highest_compensation").alias("board_member_highest_compensation"),
                # col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
                # col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
                # col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
                col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
                col("board_member.OfficerInd").alias("board_member_officer_ind"),
                col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
                # col("board_member_total_reportable_comp_org_amt"),
                # col("board_member_total_other_compensation_amt")
            )

            # Union all board member DataFrames
            board_member_df = spark.createDataFrame([], board_member_990_df.schema) if board_member_990_df else spark.createDataFrame([], board_member_990ez_df.schema)
            if board_member_990_df:
                board_member_df = board_member_df.union(board_member_990_df)
            board_member_df = board_member_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
            # # Log final board member count
            total_board_member_df_count = board_member_df.count()
            logger.info(f"Total Board Member rows: {total_board_member_df_count}")
            if total_board_member_df_count == 0:
                logger.warning(f"No board member data for {input_path}") 
            
            

            # Grantor DataFrame for 990 from form_990_df
            grantor_990_df = form_990_df.select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
                col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
                when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
                     (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
                     concat_ws(" ",
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
                     )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
                col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
                # col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
                coalesce(
                    col("ReturnHeader.Filer.PhoneNum"),
                    col("ReturnHeader.Filer.ForeignPhoneNum")
                ).alias("grantor_phone_num"),
                # col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
                when((col("ReturnHeader.Filer.USAddress.AddressLine2Txt").isNotNull()) & (col("ReturnHeader.Filer.USAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("ReturnHeader.Filer.USAddress.AddressLine1Txt"), col("ReturnHeader.Filer.USAddress.AddressLine2Txt")))
                .otherwise(col("ReturnHeader.Filer.USAddress.AddressLine1Txt")).alias("grantor_address"),
                col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
                col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
                col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
                col("ReturnData.IRS990.WebsiteAddressTxt").alias("website"),
                col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
                col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
                col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
                col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
                col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
                col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
                col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
                col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions"),
                col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
                col("ReturnData.IRS990.TotalFunctionalExpensesGrp.ManagementAndGeneralAmt").alias("total_administrative_expenses"),
                # lit(None).cast(StringType()).alias("primary_purpose_statement"),
                col("ReturnData.IRS990.NoncashContributionsAmt").alias("noncash_contributions"),  # Added
                col("ReturnData.IRS990.CYProgramServiceRevenueAmt").alias("program_service_revenue"),  # Added
                col("ReturnData.IRS990.CYInvestmentIncomeAmt").alias("investment_income"),  # Added
                col("ReturnData.IRS990.CYOtherRevenueAmt").alias("other_revenue"),  # Added
                col("ReturnData.IRS990.CYTotalRevenueAmt").alias("total_revenue"),  # Added
                col("ReturnData.IRS990.CYGrantsAndSimilarPaidAmt").alias("contributions_and_grants_paid"),  # Added
                col("ReturnData.IRS990.CYTotalExpensesAmt").alias("total_expenses"),
                col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
                col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
                col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
                col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
                col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
                lit(None).cast(StringType()).alias("board_member_highest_compensation"),
                lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
                lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
                lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
                col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
                col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
            )
            
            # Grantor DataFrame for 990EZ and 990PF from df2
            grantor_other_df = df2.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF")).select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
                col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
                when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
                     (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
                     concat_ws(" ",
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
                     )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
                col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
                # col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
                coalesce(
                    col("ReturnHeader.Filer.PhoneNum"),
                    col("ReturnHeader.Filer.ForeignPhoneNum")
                ).alias("grantor_phone_num"),
                # col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
                when((col("ReturnHeader.Filer.USAddress.AddressLine2Txt").isNotNull()) & (col("ReturnHeader.Filer.USAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("ReturnHeader.Filer.USAddress.AddressLine1Txt"), col("ReturnHeader.Filer.USAddress.AddressLine2Txt")))
                .otherwise(col("ReturnHeader.Filer.USAddress.AddressLine1Txt")).alias("grantor_address"),
                col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
                col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
                col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
                .otherwise(lit(None).cast(StringType())).alias("website"),
                lit(None).cast(StringType()).alias("total_volunteers"),
                # lit(None).cast(StringType()).alias("mission_statement"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF", 
                     lit(None).cast(StringType()))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ", 
                      col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt"))
                .otherwise(lit(None).cast(StringType())).alias("mission_statement"),
                lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
                # when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                #      col("ReturnData.IRS990EZ.MembershipDuesAmt"))
                # .otherwise(lit(None).cast(DoubleType())).alias("membership_dues"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.MembershipDuesAndAssmntGrp.UnrelatedBusinessTaxblIncmAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.MembershipDuesAndAssmntGrp.ExclusionAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.MembershipDuesAndAssmntGrp.RelatedOrExemptFunctionIncmAmt"), lit(0.0)))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.MembershipDuesAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("membership_dues"),
                when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                     col("ReturnData.IRS990EZ.FundraisingGrossIncomeAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("fundraising_contributions"),
                lit(None).cast(DoubleType()).alias("related_organization_support"),
                lit(None).cast(DoubleType()).alias("government_grants"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.ContriRcvdRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.ContributionsGiftsGrantsEtcAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("other_contributions"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("grantee_total_amount_in_year"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.TotOprExpensesRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      coalesce(col("ReturnData.IRS990EZ.SalariesOtherCompEmplBnftAmt"), lit(0.0)) +
                      coalesce(col("ReturnData.IRS990EZ.FeesAndOtherPymtToIndCntrctAmt"), lit(0.0)) +
                      coalesce(col("ReturnData.IRS990EZ.OccupancyRentUtltsAndMaintAmt"), lit(0.0)) +
                      coalesce(col("ReturnData.IRS990EZ.PrintingPublicationsPostageAmt"), lit(0.0)) +
                      coalesce(col("ReturnData.IRS990EZ.OtherExpensesTotalAmt"), lit(0.0)))
                .otherwise(lit(None).cast(DoubleType())).alias("total_administrative_expenses"),
                # when(col("ReturnHeader.ReturnTypeCd") == "990PF", 
                #      lit(None).cast(StringType()))
                # .when(col("ReturnHeader.ReturnTypeCd") == "990EZ", 
                #       col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt"))
                # .otherwise(lit(None).cast(StringType())).alias("primary_purpose_statement"),
                lit(None).cast(DoubleType()).alias("noncash_contributions"),  # Added, NULL for 990EZ/990PF
                # when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                #      coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.SubtotalsIncmProducingActyGrp.UnrelatedBusinessTaxblIncmAmt"), lit(0.0)) +
                #      coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.SubtotalsIncmProducingActyGrp.ExclusionAmt"), lit(0.0)) +
                #      coalesce(col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.SubtotalsIncmProducingActyGrp.RelatedOrExemptFunctionIncmAmt"), lit(0.0)))
                # .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                #       col("ReturnData.IRS990EZ.ProgramServiceRevenueAmt"))
                # .otherwise(lit(None).cast(DoubleType())).alias("program_service_revenue"),  # Added
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     coalesce(
                         aggregate(
                             col("ReturnData.IRS990PF.AnalysisIncomeProducingActyGrp.ProgramServiceRevenueDtl"),
                             lit(0.0),
                             lambda acc, x: acc + coalesce(x["UnrelatedBusinessTaxblIncmAmt"], lit(0.0)) +
                                                  coalesce(x["ExclusionAmt"], lit(0.0)) +
                                                  coalesce(x["RelatedOrExemptFunctionIncmAmt"], lit(0.0))
                         ),
                         lit(0.0)
                     ))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.ProgramServiceRevenueAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("program_service_revenue"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     coalesce(col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.InterestOnSavRevAndExpnssAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.DividendsRevAndExpnssAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.NetRentalIncomeOrLossAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.NetGainSaleAstRevAndExpnssAmt"), lit(0.0)) +
                     coalesce(col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.GrossProfitRevAndExpnssAmt"), lit(0.0)))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.InvestmentIncomeAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("investment_income"),  # Added
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.OtherIncomeRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.OtherRevenueTotalAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("other_revenue"),  # Added
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.TotalRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.TotalRevenueAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("total_revenue"),  # Added
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.ContriPaidRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("contributions_and_grants_paid"),  # Added
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.TotalExpensesRevAndExpnssAmt"))
                .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
                      col("ReturnData.IRS990EZ.TotalExpensesAmt"))
                .otherwise(lit(None).cast(DoubleType())).alias("total_expenses"),
                lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
                lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
                lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
                lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
                lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt"))
                .otherwise(lit(None).cast(StringType())).alias("board_member_highest_compensation"), 
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                      col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt"))
                .otherwise(lit(None).cast(StringType())).alias("board_member_over_50k_employee_count"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt"))
                .otherwise(lit(None).cast(StringType())).alias("board_member_highest_paid_contractor"),
                when(col("ReturnHeader.ReturnTypeCd") == "990PF",
                     col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt"))
                .otherwise(lit(None).cast(StringType())).alias("board_member_over_50k_contractor_count"),
                lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
                lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt")
                
            )
            
            
            
            # Union the grantor DataFrames
            grantor_df = grantor_990_df.union(grantor_other_df).na.fill({"grantor_business_name": "", "grantor_address": ""})
            
            # # Log final grantor count
            total_grantor_df_count = grantor_df.count()
            logger.info(f"Total Grantor rows: {total_grantor_df_count}")
            if total_grantor_df_count == 0:
                logger.warning(f"No grantor data for {input_path}")
            
            
            # Employee DataFrame (990PF only)
            employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                lit("990PF").alias("return_type_cd"),
                explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                col("employee.PersonNm").alias("employee_name"),
                when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
                .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
                col("employee.USAddress.CityNm").alias("employee_city"),
                col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
                col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
                col("employee.TitleTxt").alias("employee_title"),
                col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
                col("employee.CompensationAmt").alias("employee_compensation"),
                col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
                col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
            )
            
            # #Log final employee count    
            total_employee_990pf_df_count = employee_990pf_df.count()
            logger.info(f"Total employee rows: {total_employee_990pf_df_count}")
            if total_employee_990pf_df_count == 0:
                logger.warning(f"No employee data for {input_path}")
                
                
        

            # Contact DataFrame (990PF only)
            contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                lit("990PF").alias("return_type_cd"),
                explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
                when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
                .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
                col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
                col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
                col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
                col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
                col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
                col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
                col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
            )
            
            
            # #Log final contact count
            total_contact_df_count = contact_df.count()
            logger.info(f"Total contact rows: {total_contact_df_count}")
            if total_contact_df_count == 0:
                logger.warning(f"No contact data for {input_path}")
            

            # Grantee DataFrame (990PF)
            grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
                col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
                when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
                     (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
                     concat_ws(" ",
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
                     )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
                col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
                explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee")
            )

            grantee_990pf_df = grantee_990pf_df.join(
                contact_df,
                ["object_id", "grantor_ein", "tax_year"],
                "left_outer"
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                grantee_990pf_df["return_type_cd"].alias("return_type_cd"),
                col("grantor_business_name"),
                col("tax_period_begin_dt"),
                col("tax_period_end_dt"),
                col("grantor_business_name_control"),
                col("grantee.RecipientPersonNm").alias("grantee_name"),
                when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
                     (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
                     when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
                          (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
                          concat_ws(" ",
                              col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
                              col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
                          ))
                     .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
                .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
                when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
                     concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
                .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
                col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
                col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
                col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
                col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
                col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
                col("grantee.Amt").alias("grantee_amount"),
                col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
                col("grantee.RecipientEIN").alias("grantee_ein"),
                col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
                # col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
                col("grantee.TaxYr").alias("grant_paid_year"),
                # lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
                # lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
                # lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
                # lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
                # lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
                # lit(None).cast(StringType()).alias("primary_purpose_statement"),
                col("contact_person_name_for_grants"),
                col("contact_phone_num_for_grants"),
                col("contact_form_info_for_grants"),
                col("contact_submission_deadlines_for_grants"),
                col("contact_restrictions_for_grants")
            )
            
            # Log grantee row count
            grantee_990pf_count = grantee_990pf_df.count()
            logger.info(f"Form 990PF Grantee rows: {grantee_990pf_count}")
            if grantee_990pf_count == 0:
                logger.warning(f"No Form 990PF grantee data for {input_path}")

            # Grantee DataFrame (990EZ)
            grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
                col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
                col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
                when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
                     (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
                     concat_ws(" ",
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
                         col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
                     )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
                col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control")
                # col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                col("grantor_business_name"),
                col("tax_period_begin_dt"),
                col("tax_period_end_dt"),
                col("grantor_business_name_control"),
                lit(None).cast(StringType()).alias("grantee_name"),
                lit(None).cast(StringType()).alias("grantee_business_name"),
                lit(None).cast(StringType()).alias("grantee_address"),
                lit(None).cast(StringType()).alias("grantee_city"),
                lit(None).cast(StringType()).alias("grantee_state_cd"),
                lit(None).cast(StringType()).alias("grantee_zip_code"),
                lit(None).cast(StringType()).alias("grantee_foundation_status"),
                lit(None).cast(StringType()).alias("grantee_purpose"),
                lit(None).cast(DoubleType()).alias("grantee_amount"),
                lit(None).cast(StringType()).alias("grantee_relationship"),
                lit(None).cast(StringType()).alias("grantee_ein"),
                lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
                # lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
                lit(None).cast(StringType()).alias("grant_paid_year"),
                # lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
                # lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
                # lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
                # lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
                # lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
                # col("primary_purpose_statement"),
                lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
                lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
                lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
                lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
                lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
            )
            
            # Log grantee row count
            grantee_990ez_count = grantee_990ez_df.count()
            logger.info(f"Form 990EZ Grantee rows: {grantee_990ez_count}")
            if grantee_990ez_count == 0:
                logger.warning(f"No Form 990EZ grantee data for {input_path}")

            # Union Grantee DataFrames
            grantee_df = grantee_990pf_df
            if grantee_990_df:
                grantee_df = grantee_df.union(grantee_990_df)
            grantee_df = grantee_df.union(grantee_990ez_df)
            
            
            # # Log final grantee count
            total_grantee_df_count = grantee_df.count()
            logger.info(f"Total Grantee rows: {total_grantee_df_count}")
            if total_grantee_df_count == 0:
                logger.warning(f"No grantee data for {input_path}")


            # Contributor DataFrame (990PF only)
            contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
                col("object_id"),
                col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
                col("ReturnHeader.TaxYr").alias("tax_year"),
                lit("990PF").alias("return_type_cd"),
                explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
            ).select(
                col("object_id"),
                col("grantor_ein"),
                col("tax_year"),
                col("return_type_cd"),
                coalesce(
                    col("contributor.ContributorPersonNm"),
                    when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
                         (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
                         concat_ws(" ",
                             col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
                             col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
                         ))
                    .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
                ).alias("contributor_name"),
                when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
                     (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
                     concat_ws(" ",
                         col("contributor.ContributorUSAddress.AddressLine1Txt"),
                         col("contributor.ContributorUSAddress.AddressLine2Txt")
                     )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
                col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
                col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
                col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
                col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
                col("contributor.PersonContributionInd").alias("person_contribution_ind"),
                col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
                col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
            )
            
            
            # #Log final contributor count   
            total_contributor_990pf_df_count = contributor_990pf_df.count()
            logger.info(f"Total contributor rows: {total_contributor_990pf_df_count}")
            if total_contributor_990pf_df_count == 0:
                logger.warning(f"No contributor data for {input_path}")
                
            

            # Write each DataFrame to separate S3 paths
            entity_dfs = [
                ("grantor", grantor_df),
                ("board_member", board_member_df),
                ("grantee", grantee_df),
                ("contact", contact_df),
                ("employee", employee_990pf_df),
                ("contributor", contributor_990pf_df)
            ]
            
            # Map existing counts to entity_counts
            entity_counts = {
                "grantor": total_grantor_df_count,
                "board_member": total_board_member_df_count,
                "grantee": total_grantee_df_count,
                "contact": total_contact_df_count,
                "employee": total_employee_990pf_df_count,
                "contributor": total_contributor_990pf_df_count
            }

            # for entity_name, entity_df in entity_dfs:
            #     if entity_df.rdd.isEmpty():
            #         logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
            #         continue
                
            #     required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
            #     missing_columns = [col for col in required_columns if col not in entity_df.columns]
            #     if missing_columns:
            #         logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
            #         for col_name in missing_columns:
            #             entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
            #     entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
            #     try:
            #         entity_df.repartition(100).write \
            #             .mode("append") \
            #             .parquet(entity_output_path)
            #         logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
            #     except Exception as e:
            #         logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
            #         continue
            
                
            
            for entity_name, entity_df in entity_dfs:
                if entity_df.rdd.isEmpty():
                    logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
                    continue
                
                # total_count = entity_df.count()
                # display_name = entity_name.replace("_", " ").title()
                # logger.info(f"Total {display_name} rows: {total_count}")
                
                required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
                missing_columns = [col for col in required_columns if col not in entity_df.columns]
                if missing_columns:
                    logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
                    for col_name in missing_columns:
                        entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                        
                # logger.info("Adding year and subfolder started")    
                
                # Add year and subfolder columns to the DataFrame
                entity_df = entity_df.withColumn("year", lit(year)) \
                                    .withColumn("subfolder", lit(subfolder))
                                    
                # logger.info("Adding year and subfolder ended")  
                
                entity_output_path = f"{output_base_path}{entity_name}/"
                # Dynamic repartitioning: 1 partition per ~10,000 rows, max 100, min 1
                num_partitions = max(1, min(100, entity_counts[entity_name] // 10000))
                
                try:
                    entity_df.repartition(num_partitions).write \
                        .partitionBy("year", "subfolder") \
                        .mode("append") \
                        .parquet(entity_output_path)
                    logger.info(f"Written {entity_counts[entity_name]} records of {entity_name} in Parquet files to {entity_output_path}year={year}/subfolder={subfolder}/")
                except Exception as e:
                    logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
                    continue    
            
        except Exception as e:
            logger.error(f"Error processing {input_path}: {str(e)}")
            continue
        finally:
            if 'df' in locals():
                df.unpersist()

try:
    job.commit()
    logger.info("ETL job completed successfully!")
except Exception as e:
    logger.error(f"Job commit failed: {str(e)}")
    raise


# Unupdated code 16/05/25

# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }

# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_990_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("KeyEmployeeInd", StringType(), True),
#     StructField("FormerInd", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True),
#             StructField("PhoneNum", StringType(), True),
#             StructField("USAddress", StructType([
#                 StructField("AddressLine1Txt", StringType(), True),
#                 StructField("AddressLine2Txt", StringType(), True),
#                 StructField("CityNm", StringType(), True),
#                 StructField("StateAbbreviationCd", StringType(), True),
#                 StructField("ZIPCd", StringType(), True)
#             ]), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalFunctionalExpensesGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True),
#                 StructField("ProgramServicesAmt", DoubleType(), True),
#                 StructField("ManagementAndGeneralAmt", DoubleType(), True),
#                 StructField("FundraisingAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("AnalysisOfRevenueAndExpenses", StructType([
#                 StructField("TotOprExpensesRevAndExpnssAmt", DoubleType(), True)
#             ]), True),
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#             StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalFunctionalExpensesGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True),
#                 StructField("ProgramServicesAmt", DoubleType(), True),
#                 StructField("ManagementAndGeneralAmt", DoubleType(), True),
#                 StructField("FundraisingAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("SalariesOtherCompEmplBnftAmt", DoubleType(), True),
#             StructField("FeesAndOtherPymtToIndCntrctAmt", DoubleType(), True),
#             StructField("OccupancyRentUtltsAndMaintAmt", DoubleType(), True),
#             StructField("PrintingPublicationsPostageAmt", DoubleType(), True),
#             StructField("OtherExpensesTotalAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/"
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Currently processing folder: s3://{bucket_name}/{s3_prefix}")
        
#         # Check if files exist in the S3 path
#         try:
#             response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
#             if 'Contents' not in response or len(response['Contents']) == 0:
#                 logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#                 continue
#         except s3_client.exceptions.ClientError as e:
#             logger.error(f"Error listing files in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#             continue

#         try:
#             # Initialize empty DataFrames
#             board_member_990_df = None
#             grantee_990_df = None

#             # Read Form 990 for board members and grantees
#             form_990_df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(form_990_grantee_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name()) \
#                 .filter(col("ReturnHeader.ReturnTypeCd") == "990")
            
#             # Extract object_id
#             form_990_df = form_990_df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
#             # Log row count
#             form_990_count = form_990_df.count()
#             logger.info(f"Form 990 rows: {form_990_count}")
#             if form_990_count == 0:
#                 logger.warning(f"No Form 990 data in {input_path}")
#             else:
#                 # Form 990 Board Member DataFrame
#                 board_member_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                     col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                     col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                     lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#                 ).select(
#                     col("object_id"),
#                     col("grantor_ein"),
#                     col("tax_year"),
#                     col("return_type_cd"),
#                     col("board_member.PersonNm").alias("board_member_person_name"),
#                     col("board_member.TitleTxt").alias("board_member_title"),
#                     lit(None).cast(StringType()).alias("board_member_address"),
#                     lit(None).cast(StringType()).alias("board_member_city"),
#                     lit(None).cast(StringType()).alias("board_member_state_cd"),
#                     lit(None).cast(StringType()).alias("board_member_zip_code"),
#                     col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                     col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                     lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                     col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
#                     col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                     col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("board_member_highest_compensation"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
#                     col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                     col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                     col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                     col("board_member_total_reportable_comp_org_amt"),
#                     col("board_member_total_other_compensation_amt")
#                 )
                
#                 # Log board member row count
#                 count_990 = board_member_990_df.count()
#                 logger.info(f"Form 990 Board Member rows: {count_990}")
#                 if count_990 == 0:
#                     logger.warning(f"No Form 990 board member data for {input_path}")

#                 # Form 990 Grantee DataFrame
#                 grantee_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                     col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                     when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                          (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                          )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                     col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                     explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                     col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                     col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other")
#                     # col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                     # col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                     # col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                     # col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                     # col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                     # col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                     # col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                     # col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#                 ).select(
#                     col("object_id"),
#                     col("grantor_ein"),
#                     col("tax_year"),
#                     col("return_type_cd"),
#                     col("grantor_business_name"),
#                     col("tax_period_begin_dt"),
#                     col("tax_period_end_dt"),
#                     col("grantor_business_name_control"),
#                     coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#                     when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                          (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                          when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                               (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                               concat_ws(" ",
#                                   col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                                   col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                               ))
#                          .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                     .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
#                     when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                          concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                     .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                     col("grantee.USAddress.CityNm").alias("grantee_city"),
#                     col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                     col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                     col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                     col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                     coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt")).alias("grantee_amount"),
#                     col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                     col("grantee.RecipientEIN").alias("grantee_ein"),
#                     col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                     # col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                     lit(None).cast(StringType()).alias("grant_paid_year"),
#                     col("grantee_501c3_org_count"),
#                     col("grantee_other_org_count"),
#                     col("grantee_individual_grant_type"),
#                     col("grantee_individual_recipient_count"),
#                     col("grantee_individual_cash_amount_other"),
#                     # col("total_volunteers"),
#                     # col("mission_statement"),
#                     # col("federal_campaign_contributions"),
#                     # col("membership_dues"),
#                     # col("fundraising_contributions"),
#                     # col("related_organization_support"),
#                     # col("government_grants"),
#                     # col("other_contributions"),
#                     # lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                     lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#                 )
                
#                 # Log grantee row count
#                 grantee_990_count = grantee_990_df.count()
#                 logger.info(f"Form 990 Grantee rows: {grantee_990_count}")
#                 if grantee_990_count == 0:
#                     logger.warning(f"No Form 990 grantee data for {input_path}")

#             # Read with unified schema for 990EZ and 990PF
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 continue

#             df2 = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))
            
#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF"))

#             # Form 990EZ Board Member DataFrame
#             board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 # col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 # col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_average_hours_related_org"),
#                 lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 lit(None).cast(DoubleType()).alias("board_member_reportable_comp_related_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                 # col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 # col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Form 990PF Board Member DataFrame
#             board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 # col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 lit(None).cast(DoubleType()).alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 # col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 # col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 # col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_estimated_other_compensation_amt"),
#                 col("parent_highest_compensation").alias("board_member_highest_compensation"),
#                 col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
#                 col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
#                 col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Union all board member DataFrames
#             board_member_df = spark.createDataFrame([], board_member_990_df.schema) if board_member_990_df else spark.createDataFrame([], board_member_990ez_df.schema)
#             if board_member_990_df:
#                 board_member_df = board_member_df.union(board_member_990_df)
#             board_member_df = board_member_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
#             # Log final board member count
#             total_count = board_member_df.count()
#             logger.info(f"Total Board Member rows: {total_count}")
#             if total_count == 0:
#                 logger.warning(f"No board member data for {input_path}")

#             # Grantor DataFrame for 990 from form_990_df
#             grantor_990_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 col("ReturnData.IRS990.WebsiteAddressTxt").alias("website"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalFunctionalExpensesGrp.ManagementAndGeneralAmt").alias("total_administrative_expenses"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement")
#             )
            
#             # Grantor DataFrame for 990EZ and 990PF from df2
#             grantor_other_df = df2.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF")).select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("grantee_total_amount_in_year"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.TotOprExpensesRevAndExpnssAmt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       coalesce(col("ReturnData.IRS990EZ.SalariesOtherCompEmplBnftAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.FeesAndOtherPymtToIndCntrctAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.OccupancyRentUtltsAndMaintAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.PrintingPublicationsPostageAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.OtherExpensesTotalAmt"), lit(0.0)))
#                 .otherwise(lit(None).cast(DoubleType())).alias("total_administrative_expenses"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF", 
#                      lit(None).cast(StringType()))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ", 
#                       col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("primary_purpose_statement")
#             )
            
#             # Union the grantor DataFrames
#             grantor_df = grantor_990_df.union(grantor_other_df).na.fill({"grantor_business_name": "", "grantor_address": ""})
            
#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )

#             # Drop return_type_cd from contact_df to avoid ambiguity
#             contact_df = contact_df.drop("return_type_cd")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee")
#             )

#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 # col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 # lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990pf_count = grantee_990pf_df.count()
#             logger.info(f"Form 990PF Grantee rows: {grantee_990pf_count}")
#             if grantee_990pf_count == 0:
#                 logger.warning(f"No Form 990PF grantee data for {input_path}")

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control")
#                 # col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 # lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 # col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990ez_count = grantee_990ez_df.count()
#             logger.info(f"Form 990EZ Grantee rows: {grantee_990ez_count}")
#             if grantee_990ez_count == 0:
#                 logger.warning(f"No Form 990EZ grantee data for {input_path}")

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df
#             if grantee_990_df:
#                 grantee_df = grantee_df.union(grantee_990_df)
#             grantee_df = grantee_df.union(grantee_990ez_df)

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             if 'df' in locals():
#                 df.unpersist()

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise



################# Final code with fixes including grantor issue

# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_990_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("KeyEmployeeInd", StringType(), True),
#     StructField("FormerInd", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#             StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         try:
#             response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
#             if 'Contents' not in response or len(response['Contents']) == 0:
#                 logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#                 continue
#             logger.info(f"Files found in s3://{bucket_name}/{s3_prefix}")
#         except s3_client.exceptions.ClientError as e:
#             logger.error(f"Error listing files in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#             continue

#         try:
#             # Initialize empty DataFrames
#             board_member_990_df = None
#             grantee_990_df = None
#             grantor_990_df = None

#             # Read Form 990 for board members and grantees
#             form_990_df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(form_990_grantee_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name()) \
#                 .filter(col("ReturnHeader.ReturnTypeCd") == "990")
            
#             # Extract object_id
#             form_990_df = form_990_df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
#             # Log row count
#             form_990_count = form_990_df.count()
#             logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#             if form_990_count == 0:
#                 logger.warning(f"No Form 990 data in {input_path}")
#             else:
#                 # Form 990 Board Member DataFrame
#                 board_member_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                     col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                     col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                     lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#                 ).select(
#                     col("object_id"),
#                     col("grantor_ein"),
#                     col("tax_year"),
#                     col("return_type_cd"),
#                     col("board_member.PersonNm").alias("board_member_person_name"),
#                     col("board_member.TitleTxt").alias("board_member_title"),
#                     lit(None).cast(StringType()).alias("board_member_address"),
#                     lit(None).cast(StringType()).alias("board_member_city"),
#                     lit(None).cast(StringType()).alias("board_member_state_cd"),
#                     lit(None).cast(StringType()).alias("board_member_zip_code"),
#                     col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                     col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                     lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                     col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
#                     col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                     col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("board_member_highest_compensation"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
#                     col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                     col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                     col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                     col("board_member_total_reportable_comp_org_amt"),
#                     col("board_member_total_other_compensation_amt")
#                 )
                
#                 # Log board member row count
#                 count_990 = board_member_990_df.count()
#                 logger.info(f"Form 990 Board Member rows for {input_path}: {count_990}")
#                 if count_990 == 0:
#                     logger.warning(f"Empty board_member_990_df for {input_path}")

#                 # Form 990 Grantee DataFrame
#                 grantee_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                     col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                     when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                          (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                          )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                     col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                     explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                     col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                     col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                     col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                     col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                     col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                     col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions")
#                 ).na.fill({"grantor_business_name": ""})
#                 logger.info(f"Extracted Form 990 grantee information for {input_path}")

#                 # Form 990 Grantor DataFrame
#                 try:
#                     grantor_990_df = form_990_df.select(
#                         col("object_id"),
#                         col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                         col("ReturnHeader.TaxYr").alias("tax_year"),
#                         col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                         col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                         col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                         when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                              (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                              concat_ws(" ",
#                                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                              )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                         col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                         lit(None).cast(StringType()).alias("grantor_phone_num"),
#                         lit(None).cast(StringType()).alias("grantor_address"),
#                         lit(None).cast(StringType()).alias("grantor_city"),
#                         lit(None).cast(StringType()).alias("grantor_state_cd"),
#                         lit(None).cast(StringType()).alias("grantor_zip_code"),
#                         col("ReturnData.IRS990.WebsiteAddressTxt").alias("website")
#                     ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#                     logger.info(f"Extracted Form 990 grantor information for {input_path}")
#                     grantor_990_count = grantor_990_df.count()
#                     logger.info(f"Form 990 Grantor rows for {input_path}: {grantor_990_count}")
#                     if grantor_990_count == 0:
#                         logger.warning(f"No Form 990 grantor data extracted for {input_path}")
#                 except Exception as e:
#                     logger.error(f"Error processing Form 990 grantor data in {input_path}: {str(e)}")
#                     grantor_990_df = None
#                     logger.warning(f"Skipping Form 990 grantor processing for {input_path} due to error")

#             # Read with unified schema for 990EZ and 990PF
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with unified schema")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF"))

#             # Form 990EZ Board Member DataFrame
#             board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Form 990PF Board Member DataFrame
#             board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("parent_highest_compensation").alias("board_member_highest_compensation"),
#                 col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
#                 col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
#                 col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Union all board member DataFrames
#             board_member_df = spark.createDataFrame([], board_member_990_df.schema) if board_member_990_df else spark.createDataFrame([], board_member_990ez_df.schema)
#             if board_member_990_df:
#                 board_member_df = board_member_df.union(board_member_990_df)
#             board_member_df = board_member_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
#             # Log final board member count
#             total_count = board_member_df.count()
#             logger.info(f"Total Board Member rows for {input_path}: {total_count}")
#             if total_count == 0:
#                 logger.warning(f"Empty final board_member_df for {input_path}")

#             # Grantor DataFrame (990EZ and 990PF)
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info(f"Extracted 990EZ and 990PF grantor information for {input_path}")

#             # Union Form 990 grantor data if available
#             if grantor_990_df:
#                 grantor_df = grantor_df.union(grantor_990_df)
#                 logger.info(f"Unioned Form 990 grantor data with 990EZ and 990PF for {input_path}")
#             grantor_count = grantor_df.count()
#             logger.info(f"Total Grantor rows for {input_path}: {grantor_count}")
#             if grantor_count == 0:
#                 logger.warning(f"No grantor data extracted for {input_path}")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")
#             logger.info(f"Contact DataFrame schema before join: {contact_df.schema}")

#             # Drop return_type_cd from contact_df to avoid ambiguity
#             contact_df = contact_df.drop("return_type_cd")
#             logger.info(f"Contact DataFrame schema after dropping return_type_cd: {contact_df.schema}")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year")
#             )
#             logger.info(f"Grantee 990PF DataFrame schema before join: {grantee_990pf_df.schema}")

#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990pf_count = grantee_990pf_df.count()
#             logger.info(f"Form 990PF Grantee rows for {input_path}: {grantee_990pf_count}")
#             if grantee_990pf_count == 0:
#                 logger.warning(f"No 990PF grantee data extracted for {input_path}")

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990ez_count = grantee_990ez_df.count()
#             logger.info(f"Form 990EZ Grantee rows for {input_path}: {grantee_990ez_count}")
#             if grantee_990ez_count == 0:
#                 logger.warning(f"No 990EZ grantee data extracted for {input_path}")

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df
#             if grantee_990_df:
#                 grantee_df = grantee_df.union(grantee_990_df)
#             grantee_df = grantee_df.union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             if 'df' in locals():
#                 df.unpersist()
#                 logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise








# ##### New code after all the fixes everything seems fine########################

# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# # years = ["2019", "2020"]
# years = ["2020"]
# subfolders = {
#     # "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_990_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("KeyEmployeeInd", StringType(), True),
#     StructField("FormerInd", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True),
#             StructField("PhoneNum", StringType(), True),
#             StructField("USAddress", StructType([
#                 StructField("AddressLine1Txt", StringType(), True),
#                 StructField("AddressLine2Txt", StringType(), True),
#                 StructField("CityNm", StringType(), True),
#                 StructField("StateAbbreviationCd", StringType(), True),
#                 StructField("ZIPCd", StringType(), True)
#             ]), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalFunctionalExpensesGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True),
#                 StructField("ProgramServicesAmt", DoubleType(), True),
#                 StructField("ManagementAndGeneralAmt", DoubleType(), True),
#                 StructField("FundraisingAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])



# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("AnalysisOfRevenueAndExpenses", StructType([
#                 StructField("TotOprExpensesRevAndExpnssAmt", DoubleType(), True)
#             ]), True),
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#             StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalFunctionalExpensesGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True),
#                 StructField("ProgramServicesAmt", DoubleType(), True),
#                 StructField("ManagementAndGeneralAmt", DoubleType(), True),
#                 StructField("FundraisingAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("SalariesOtherCompEmplBnftAmt", DoubleType(), True),
#             StructField("FeesAndOtherPymtToIndCntrctAmt", DoubleType(), True),
#             StructField("OccupancyRentUtltsAndMaintAmt", DoubleType(), True),
#             StructField("PrintingPublicationsPostageAmt", DoubleType(), True),
#             StructField("OtherExpensesTotalAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         try:
#             response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
#             if 'Contents' not in response or len(response['Contents']) == 0:
#                 logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#                 continue
#             logger.info(f"Files found in s3://{bucket_name}/{s3_prefix}")
#         except s3_client.exceptions.ClientError as e:
#             logger.error(f"Error listing files in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#             continue

#         try:
#             # Initialize empty DataFrames
#             board_member_990_df = None
#             grantee_990_df = None

#             # Read Form 990 for board members and grantees
#             form_990_df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(form_990_grantee_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name()) \
#                 .filter(col("ReturnHeader.ReturnTypeCd") == "990")
            
#             # Extract object_id
#             form_990_df = form_990_df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
            
            
#             # logger.info(f"form_990_df schema for {input_path}:")
#             # form_990_df.printSchema()
#             # logger.info(f"Sample df2 data for Foundation for the Carolinas:")
#             # form_990_df.filter(col("ReturnHeader.Filer.EIN") == "566047886").select(
#             #     "ReturnHeader.ReturnTypeCd",
#             #     "ReturnData.IRS990.TotalVolunteersCnt",
#             #     "ReturnData.IRS990.ActivityOrMissionDesc",
#             #     "ReturnData.IRS990.FederatedCampaignsAmt",
#             #     "ReturnData.IRS990.MembershipDuesAmt",
#             #     "ReturnData.IRS990.FundraisingAmt",
#             #     "ReturnData.IRS990.RelatedOrganizationsAmt",
#             #     "ReturnData.IRS990.GovernmentGrantsAmt",
#             #     "ReturnData.IRS990.AllOtherContributionsAmt"
#             # ).show(5, truncate=False)
            
            
#             # logger.info(f"Sample form_990_df data for EIN 621269372:")
#             # form_990_df.filter(col("ReturnHeader.Filer.EIN") == "621269372").select(
#             #     "ReturnHeader.ReturnTypeCd",
#             #     "ReturnData.IRS990.TotalVolunteersCnt",
#             #     "ReturnData.IRS990.ActivityOrMissionDesc",
#             #     "ReturnData.IRS990.FederatedCampaignsAmt",
#             #     "ReturnData.IRS990.MembershipDuesAmt",
#             #     "ReturnData.IRS990.FundraisingAmt",
#             #     "ReturnData.IRS990.RelatedOrganizationsAmt",
#             #     "ReturnData.IRS990.GovernmentGrantsAmt",
#             #     "ReturnData.IRS990.AllOtherContributionsAmt"
#             # ).show(5, truncate=False)
            
            
            
#             # Log row count
#             form_990_count = form_990_df.count()
#             logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#             if form_990_count == 0:
#                 logger.warning(f"No Form 990 data in {input_path}")
#             else:
#                 # Form 990 Board Member DataFrame
#                 board_member_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                     col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                     col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                     lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#                 ).select(
#                     col("object_id"),
#                     col("grantor_ein"),
#                     col("tax_year"),
#                     col("return_type_cd"),
#                     col("board_member.PersonNm").alias("board_member_person_name"),
#                     col("board_member.TitleTxt").alias("board_member_title"),
#                     lit(None).cast(StringType()).alias("board_member_address"),
#                     lit(None).cast(StringType()).alias("board_member_city"),
#                     lit(None).cast(StringType()).alias("board_member_state_cd"),
#                     lit(None).cast(StringType()).alias("board_member_zip_code"),
#                     col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                     col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                     lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                     col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
#                     col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                     lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                     col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                     lit(None).cast(StringType()).alias("board_member_highest_compensation"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
#                     lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
#                     lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
#                     col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                     col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                     col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                     col("board_member_total_reportable_comp_org_amt"),
#                     col("board_member_total_other_compensation_amt")
#                 )
                
#                 # Log board member row count
#                 count_990 = board_member_990_df.count()
#                 logger.info(f"Form 990 Board Member rows for {input_path}: {count_990}")
#                 if count_990 == 0:
#                     logger.warning(f"Empty board_member_990_df for {input_path}")

#                 # Form 990 Grantee DataFrame
#                 grantee_990_df = form_990_df.select(
#                     col("object_id"),
#                     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                     col("ReturnHeader.TaxYr").alias("tax_year"),
#                     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                     col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                     col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                     when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                          (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                              col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                          )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                     col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                     explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                     col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                     col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                     col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                     # col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                     col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                     col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                     col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                     col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                     col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                     col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                     col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                     col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#                 ).select(
#                     col("object_id"),
#                     col("grantor_ein"),
#                     col("tax_year"),
#                     col("return_type_cd"),
#                     col("grantor_business_name"),
#                     col("tax_period_begin_dt"),
#                     col("tax_period_end_dt"),
#                     col("grantor_business_name_control"),
#                     coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#                     when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                          (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                          when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                               (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                               concat_ws(" ",
#                                   col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                                   col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                               ))
#                          .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                     .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
#                     when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                          concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                     .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                     col("grantee.USAddress.CityNm").alias("grantee_city"),
#                     col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                     col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                     col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                     col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                     coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt")).alias("grantee_amount"),
#                     # col("grantee_total_amount_in_year"),
#                     col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                     col("grantee.RecipientEIN").alias("grantee_ein"),
#                     col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                     col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                     lit(None).cast(StringType()).alias("grant_paid_year"),
#                     col("grantee_501c3_org_count"),
#                     col("grantee_other_org_count"),
#                     col("grantee_individual_grant_type"),
#                     col("grantee_individual_recipient_count"),
#                     col("grantee_individual_cash_amount_other"),
#                     col("total_volunteers"),
#                     col("mission_statement"),
#                     col("federal_campaign_contributions"),
#                     col("membership_dues"),
#                     col("fundraising_contributions"),
#                     col("related_organization_support"),
#                     col("government_grants"),
#                     col("other_contributions"),
#                     lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                     lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                     lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#                 )
                
#                 # Log grantee row count
#                 grantee_990_count = grantee_990_df.count()
#                 logger.info(f"Form 990 Grantee rows for {input_path}: {grantee_990_count}")
#                 if grantee_990_count == 0:
#                     logger.warning(f"No grantee data extracted for {input_path}")

#             # Read with unified schema for 990EZ and 990PF
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with unified schema")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df2 = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))
            
            
#             # logger.info(f"df2 schema for {input_path}:")
#             # df2.printSchema()
#             # logger.info(f"Sample df2 data for Foundation for the Carolinas:")
#             # df2.filter(col("ReturnHeader.Filer.EIN") == "566047886").select(
#             #     "ReturnHeader.ReturnTypeCd",
#             #     "ReturnData.IRS990.TotalVolunteersCnt",
#             #     "ReturnData.IRS990.ActivityOrMissionDesc",
#             #     "ReturnData.IRS990.FederatedCampaignsAmt",
#             #     "ReturnData.IRS990.MembershipDuesAmt",
#             #     "ReturnData.IRS990.FundraisingAmt",
#             #     "ReturnData.IRS990.RelatedOrganizationsAmt",
#             #     "ReturnData.IRS990.GovernmentGrantsAmt",
#             #     "ReturnData.IRS990.AllOtherContributionsAmt"
#             # ).show(5, truncate=False)
            
            
#             # logger.info(f"Sample df2 data for EIN 621269372:")
#             # df2.filter(col("ReturnHeader.Filer.EIN") == "621269372").select(
#             #     "ReturnHeader.ReturnTypeCd",
#             #     "ReturnData.IRS990.TotalVolunteersCnt",
#             #     "ReturnData.IRS990.ActivityOrMissionDesc",
#             #     "ReturnData.IRS990.FederatedCampaignsAmt",
#             #     "ReturnData.IRS990.MembershipDuesAmt",
#             #     "ReturnData.IRS990.FundraisingAmt",
#             #     "ReturnData.IRS990.RelatedOrganizationsAmt",
#             #     "ReturnData.IRS990.GovernmentGrantsAmt",
#             #     "ReturnData.IRS990.AllOtherContributionsAmt"
#             # ).show(5, truncate=False)
                        
            

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF"))

#             # Form 990EZ Board Member DataFrame
#             board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Form 990PF Board Member DataFrame
#             board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("parent_highest_compensation").alias("board_member_highest_compensation"),
#                 col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
#                 col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
#                 col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Union all board member DataFrames
#             board_member_df = spark.createDataFrame([], board_member_990_df.schema) if board_member_990_df else spark.createDataFrame([], board_member_990ez_df.schema)
#             if board_member_990_df:
#                 board_member_df = board_member_df.union(board_member_990_df)
#             board_member_df = board_member_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
#             # Log final board member count
#             total_count = board_member_df.count()
#             logger.info(f"Total Board Member rows for {input_path}: {total_count}")
#             if total_count == 0:
#                 logger.warning(f"Empty final board_member_df for {input_path}")

#             # Grantor DataFrame
            
#             # grantor_df = df2.select(
#             #     col("object_id"),
#             #     col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#             #     col("ReturnHeader.TaxYr").alias("tax_year"),
#             #     col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#             #     col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#             #     col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#             #     when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#             #          (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#             #          concat_ws(" ",
#             #              col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#             #              col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#             #          )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#             #     col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#             #     col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#             #     col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#             #     col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#             #     col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#             #     col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#             #          col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#             #     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #           col("ReturnData.IRS990.WebsiteAddressTxt"))
#             #     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#             #           col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#             #     .otherwise(lit(None).cast(StringType())).alias("website"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.TotalVolunteersCnt"))
#             #     .otherwise(lit(None).cast(StringType())).alias("total_volunteers"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.ActivityOrMissionDesc"))
#             #     .otherwise(lit(None).cast(StringType())).alias("mission_statement"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.FederatedCampaignsAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("federal_campaign_contributions"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.MembershipDuesAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("membership_dues"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.FundraisingAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("fundraising_contributions"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.RelatedOrganizationsAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("related_organization_support"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.GovernmentGrantsAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("government_grants"),
#             #     when(col("ReturnHeader.ReturnTypeCd") == "990",
#             #          col("ReturnData.IRS990.AllOtherContributionsAmt"))
#             #     .otherwise(lit(None).cast(DoubleType())).alias("other_contributions")
                
#             # ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             # logger.info("Extracted grantor information")
            
            
            
#             # Grantor DataFrame for 990 from form_990_df
#             grantor_990_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 col("ReturnData.IRS990.WebsiteAddressTxt").alias("website"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalFunctionalExpensesGrp.ManagementAndGeneralAmt").alias("total_administrative_expenses")
#             )
            
#             # Grantor DataFrame for 990EZ and 990PF from df2
#             grantor_other_df = df2.filter(col("ReturnHeader.ReturnTypeCd").isin("990EZ", "990PF")).select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("grantee_total_amount_in_year"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.AnalysisOfRevenueAndExpenses.TotOprExpensesRevAndExpnssAmt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       coalesce(col("ReturnData.IRS990EZ.SalariesOtherCompEmplBnftAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.FeesAndOtherPymtToIndCntrctAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.OccupancyRentUtltsAndMaintAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.PrintingPublicationsPostageAmt"), lit(0.0)) +
#                       coalesce(col("ReturnData.IRS990EZ.OtherExpensesTotalAmt"), lit(0.0)))
#                 .otherwise(lit(None).cast(DoubleType())).alias("total_administrative_expenses")
                
#             )
            
#             # Union the grantor DataFrames
#             grantor_df = grantor_990_df.union(grantor_other_df).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")
            
            
            

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")
#             logger.info(f"Contact DataFrame schema before join: {contact_df.schema}")

#             # Drop return_type_cd from contact_df to avoid ambiguity
#             contact_df = contact_df.drop("return_type_cd")
#             logger.info(f"Contact DataFrame schema after dropping return_type_cd: {contact_df.schema}")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee")
#                 # col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year")
#             )
#             logger.info(f"Grantee 990PF DataFrame schema before join: {grantee_990pf_df.schema}")

#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),  # Directly reference return_type_cd from grantee_990pf_df
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 # col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 # lit(None).cast(StringType()).alias("total_volunteers"),
#                 # lit(None).cast(StringType()).alias("mission_statement"),
#                 # lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 # lit(None).cast(DoubleType()).alias("membership_dues"),
#                 # lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 # lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 # lit(None).cast(DoubleType()).alias("government_grants"),
#                 # lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990pf_count = grantee_990pf_df.count()
#             logger.info(f"Form 990PF Grantee rows for {input_path}: {grantee_990pf_count}")
#             if grantee_990pf_count == 0:
#                 logger.warning(f"No 990PF grantee data extracted for {input_path}")

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 # col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 # col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 # lit(None).cast(StringType()).alias("total_volunteers"),
#                 # lit(None).cast(StringType()).alias("mission_statement"),
#                 # lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 # lit(None).cast(DoubleType()).alias("membership_dues"),
#                 # lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 # lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 # lit(None).cast(DoubleType()).alias("government_grants"),
#                 # lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_990ez_count = grantee_990ez_df.count()
#             logger.info(f"Form 990EZ Grantee rows for {input_path}: {grantee_990ez_count}")
#             if grantee_990ez_count == 0:
#                 logger.warning(f"No 990EZ grantee data extracted for {input_path}")

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df
#             if grantee_990_df:
#                 grantee_df = grantee_df.union(grantee_990_df)
#             grantee_df = grantee_df.union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             if 'df' in locals():
#                 df.unpersist()
#                 logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise












####990 grantee dataframe populated from diffrent files handling

# import sys
# import logging
# import boto3
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, input_file_name, coalesce
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "10")
#     spark.conf.set("spark.default.parallelism", "10")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_path = "s3://lumino-data/xml_data_extraction/xml_grantee_990/grantee_990/"
# bucket_name = "lumino-data"

# # Define schemas
# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("GrantAmt", DoubleType(), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True)
#         ]), True)
#     ]), True)
# ])

# # Process each year and subfolder
# s3_client = boto3.client('s3')
# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/"
#         logger.info(f"Processing files in {input_path}")

#         # Check if folder exists and has files
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         try:
#             response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
#             if 'Contents' not in response:
#                 logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}")
#                 continue
#             logger.info(f"Files found in s3://{bucket_name}/{s3_prefix}")
#         except s3_client.exceptions.ClientError as e:
#             logger.error(f"Error listing files in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#             continue

#         # Process XML files
#         try:
#             # Read Form 990 files
#             form_990_df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(form_990_grantee_schema) \
#                 .load(f"{input_path}*.xml") \
#                 .withColumn("input_file_name", input_file_name()) \
#                 .filter(col("ReturnHeader.ReturnTypeCd") == "990")
            
#             # Extract object_id
#             form_990_df = form_990_df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
#             # Log row count
#             form_990_count = form_990_df.count()
#             logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#             if form_990_count == 0:
#                 logger.warning(f"No Form 990 data in {input_path}")
#                 continue
            
#             # Extract grantee data from RecipientTable
#             grantee_990_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt"), col("grantee.GrantAmt")).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 col("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )
            
#             # Log grantee row count
#             grantee_count = grantee_990_df.count()
#             logger.info(f"Form 990 Grantee rows for {input_path}: {grantee_count}")
#             if grantee_count == 0:
#                 logger.warning(f"No grantee data extracted for {input_path}")
#                 continue
            
#             # Write output
#             try:
#                 grantee_990_df.repartition(1).write \
#                     .mode("append") \
#                     .parquet(output_path)
#                 logger.info(f"Appended grantee Parquet files to {output_path}")
#             except Exception as e:
#                 logger.error(f"Error writing grantee to {output_path}: {str(e)}")
#                 continue
#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue

# # Final job commit
# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Error committing job: {str(e)}")
#     raise




############## code working fine for the 990 grantee all cloumns of grantee dataframe

# import sys
# import logging
# import boto3
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, input_file_name, coalesce
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "10")
#     spark.conf.set("spark.default.parallelism", "10")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# input_path = "s3://lumino-data/xml_data_extraction/xml_test/2020/download990xml_2020_2/202123199349322547_public.xml"
# output_path = "s3://lumino-data/xml_data_extraction/xml_grantee_990/"
# bucket_name = "lumino-data"
# s3_prefix = "xml_data_extraction/xml_test/2020/download990xml_2020_2/202123199349322547_public.xml"

# # Check if file exists
# s3_client = boto3.client('s3')
# logger.info(f"Checking for file in s3://{bucket_name}/{s3_prefix}")
# try:
#     s3_client.head_object(Bucket=bucket_name, Key=s3_prefix)
#     logger.info(f"File found: s3://{bucket_name}/{s3_prefix}")
# except s3_client.exceptions.ClientError as e:
#     logger.error(f"File not found in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#     sys.exit(0)

# # Define schemas
# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("GrantAmt", DoubleType(), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML file: {input_path}")

# try:
#     # Read Form 990 file
#     form_990_df = spark.read \
#         .format("xml") \
#         .option("rowTag", "Return") \
#         .option("wholeFile", "true") \
#         .option("inferSchema", "false") \
#         .option("ignoreNamespace", "true") \
#         .schema(form_990_grantee_schema) \
#         .load(input_path) \
#         .withColumn("input_file_name", input_file_name()) \
#         .filter(col("ReturnHeader.ReturnTypeCd") == "990")
    
#     # Extract object_id
#     form_990_df = form_990_df.withColumn(
#         "filename",
#         substring_index(col("input_file_name"), "/", -1)
#     ).withColumn(
#         "object_id",
#         regexp_extract(col("filename"), r"(\d+)", 1)
#     ).drop("filename")
    
#     # Debug: Inspect Form 990 data
#     form_990_count = form_990_df.count()
#     logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#     if form_990_count > 0:
#         logger.info("Sample Form 990 data:")
#         form_990_df.select(
#             "ReturnHeader.Filer.EIN",
#             "ReturnHeader.ReturnTypeCd",
#             "ReturnData.IRS990ScheduleI"
#         ).show(1, truncate=False, vertical=True)
#         # Check for Foundation for the Carolinas
#         foundation_df = form_990_df.filter(col("ReturnHeader.Filer.EIN") == "566047886")
#         if foundation_df.count() > 0:
#             logger.info("Foundation for the Carolinas (EIN 566047886) found:")
#             foundation_df.select(
#                 "ReturnHeader.Filer.EIN",
#                 "ReturnData.IRS990ScheduleI.RecipientTable",
#                 "ReturnData.IRS990ScheduleI.GrantsAndAllocations"
#             ).show(1, truncate=False, vertical=True)
#         else:
#             logger.warning("Foundation for the Carolinas (EIN 566047886) not found")
#     else:
#         logger.warning(f"No Form 990 data in {input_path}")
#         sys.exit(0)
    
#     # Extract grantee data from RecipientTable
#     grantee_990_df = form_990_df.select(
#         col("object_id"),
#         col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#         col("ReturnHeader.TaxYr").alias("tax_year"),
#         col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#         col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#         col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#         when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#              (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#              concat_ws(" ",
#                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#              )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#         col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#         explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#         col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#         col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#         col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#         col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#         col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#         col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#         col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#         col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#         col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#         col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#         col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#     ).select(
#         col("object_id"),
#         col("grantor_ein"),
#         col("tax_year"),
#         col("return_type_cd"),
#         col("grantor_business_name"),
#         col("tax_period_begin_dt"),
#         col("tax_period_end_dt"),
#         col("grantor_business_name_control"),
#         coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#         when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#              (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#              when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                   (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                   concat_ws(" ",
#                       col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                       col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                   ))
#              .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#         .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
#         when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#              concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#         .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#         col("grantee.USAddress.CityNm").alias("grantee_city"),
#         col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#         col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#         col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#         col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#         coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt"), col("grantee.GrantAmt")).alias("grantee_amount"),
#         col("grantee_total_amount_in_year"),
#         col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#         col("grantee.RecipientEIN").alias("grantee_ein"),
#         col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#         col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#         lit(None).cast(StringType()).alias("grant_paid_year"),
#         col("grantee_501c3_org_count"),
#         col("grantee_other_org_count"),
#         col("grantee_individual_grant_type"),
#         col("grantee_individual_recipient_count"),
#         col("grantee_individual_cash_amount_other"),
#         col("total_volunteers"),
#         col("mission_statement"),
#         col("federal_campaign_contributions"),
#         col("membership_dues"),
#         col("fundraising_contributions"),
#         col("related_organization_support"),
#         col("government_grants"),
#         col("other_contributions"),
#         lit(None).cast(StringType()).alias("primary_purpose_statement"),
#         lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#         lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#         lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#         lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#         lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#     )
    
#     # Debug: Check grantee_990_df
#     grantee_count = grantee_990_df.count()
#     logger.info(f"Form 990 Grantee rows for {input_path}: {grantee_count}")
#     if grantee_count == 0:
#         logger.warning(f"Empty grantee_990_df for {input_path}")
#         form_990_df.select(
#             "ReturnHeader.Filer.EIN",
#             "ReturnData.IRS990ScheduleI"
#         ).show(1, truncate=False, vertical=True)
#         # Infer schema for IRS990ScheduleI
#         logger.info("Inferring schema for ReturnData.IRS990ScheduleI")
#         temp_df = spark.read \
#             .format("xml") \
#             .option("rowTag", "Return") \
#             .option("wholeFile", "true") \
#             .option("inferSchema", "true") \
#             .load(input_path)
#         temp_df.select("ReturnData.IRS990ScheduleI").printSchema()
#         # Try alternative GrantsAndAllocations
#         if form_990_df.select("ReturnData.IRS990ScheduleI.GrantsAndAllocations").count() > 0:
#             logger.info("Attempting to extract from GrantsAndAllocations")
#             alt_grantee_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990ScheduleI.GrantsAndAllocations")).alias("grantee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#                 col("grantee.RecipientBusinessName.BusinessNameLine1Txt").alias("grantee_business_name"),
#                 col("grantee.USAddress.AddressLine1Txt").alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt"), col("grantee.GrantAmt")).alias("grantee_amount")
#             )
#             alt_count = alt_grantee_df.count()
#             logger.info(f"GrantsAndAllocations rows: {alt_count}")
#             if alt_count > 0:
#                 alt_grantee_df.show(5, truncate=False)
#     else:
#         logger.info(f"Form 990 Grantee sample for {input_path}:")
#         grantee_990_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)
    
#     # Write output
#     if grantee_count > 0:
#         try:
#             grantee_990_df.repartition(1).write \
#                 .mode("overwrite") \
#                 .parquet(output_path)
#             logger.info(f"Written grantee Parquet files to {output_path}")
#         except Exception as e:
#             logger.error(f"Error writing grantee to {output_path}: {str(e)}")
#             raise
#     else:
#         logger.warning(f"Skipping write to {output_path} due to empty grantee_990_df")

# except Exception as e:
#     logger.error(f"Error processing {input_path}: {str(e)}")
#     raise
# finally:
#     job.commit()
#     logger.info("ETL job completed successfully!")






#### garntee foundation for carolinas working fine with some columns not yet added

# import sys
# import logging
# import boto3
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, input_file_name, coalesce
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "10")
#     spark.conf.set("spark.default.parallelism", "10")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# input_path = "s3://lumino-data/xml_data_extraction/xml_test/2020/download990xml_2020_2/202123199349322547_public.xml"
# output_path = "s3://lumino-data/xml_data_extraction/xml_grantee_990/"
# bucket_name = "lumino-data"
# s3_prefix = "xml_data_extraction/xml_test/2020/download990xml_2020_2/202123199349322547_public.xml"

# # Check if file exists
# s3_client = boto3.client('s3')
# logger.info(f"Checking for file in s3://{bucket_name}/{s3_prefix}")
# try:
#     s3_client.head_object(Bucket=bucket_name, Key=s3_prefix)
#     logger.info(f"File found: s3://{bucket_name}/{s3_prefix}")
# except s3_client.exceptions.ClientError as e:
#     logger.error(f"File not found in s3://{bucket_name}/{s3_prefix}: {str(e)}")
#     sys.exit(0)

# # Define schemas
# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientName", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("GrantAmt", DoubleType(), True),
# ])

# form_990_grantee_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("TaxPeriodBeginDt", StringType(), True),
#         StructField("TaxPeriodEndDt", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True),
#             StructField("BusinessName", StructType([
#                 StructField("BusinessNameLine1Txt", StringType(), True),
#                 StructField("BusinessNameLine2Txt", StringType(), True)
#             ]), True),
#             StructField("BusinessNameControlTxt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsAndAllocations", ArrayType(grantee_schema), True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", StructType([
#                 StructField("GrantTypeTxt", StringType(), True),
#                 StructField("RecipientCnt", StringType(), True),
#                 StructField("CashGrantAmt", DoubleType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML file: {input_path}")

# try:
#     # Read Form 990 file
#     form_990_df = spark.read \
#         .format("xml") \
#         .option("rowTag", "Return") \
#         .option("wholeFile", "true") \
#         .option("inferSchema", "false") \
#         .option("ignoreNamespace", "true") \
#         .schema(form_990_grantee_schema) \
#         .load(input_path) \
#         .withColumn("input_file_name", input_file_name()) \
#         .filter(col("ReturnHeader.ReturnTypeCd") == "990")
    
#     # Extract object_id
#     form_990_df = form_990_df.withColumn(
#         "filename",
#         substring_index(col("input_file_name"), "/", -1)
#     ).withColumn(
#         "object_id",
#         regexp_extract(col("filename"), r"(\d+)", 1)
#     ).drop("filename")
    
#     # Debug: Inspect Form 990 data
#     form_990_count = form_990_df.count()
#     logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#     if form_990_count > 0:
#         logger.info("Sample Form 990 data:")
#         form_990_df.select(
#             "ReturnHeader.Filer.EIN",
#             "ReturnHeader.ReturnTypeCd",
#             "ReturnData.IRS990ScheduleI"
#         ).show(1, truncate=False, vertical=True)
#         # Check for Foundation for the Carolinas
#         foundation_df = form_990_df.filter(col("ReturnHeader.Filer.EIN") == "566047886")
#         if foundation_df.count() > 0:
#             logger.info("Foundation for the Carolinas (EIN 566047886) found:")
#             foundation_df.select(
#                 "ReturnHeader.Filer.EIN",
#                 "ReturnData.IRS990ScheduleI.RecipientTable",
#                 "ReturnData.IRS990ScheduleI.GrantsAndAllocations"
#             ).show(1, truncate=False, vertical=True)
#         else:
#             logger.warning("Foundation for the Carolinas (EIN 566047886) not found")
#     else:
#         logger.warning(f"No Form 990 data in {input_path}")
#         sys.exit(0)
    
#     # Extract grantee data from RecipientTable
#     grantee_990_df = form_990_df.select(
#         col("object_id"),
#         col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#         col("ReturnHeader.TaxYr").alias("tax_year"),
#         col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#         col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#         col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#         when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#              (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#              concat_ws(" ",
#                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                  col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#              )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#         col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#         explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#         col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#         col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#         col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#         col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#         col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#         col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement")
#     ).select(
#         col("object_id"),
#         col("grantor_ein"),
#         col("tax_year"),
#         col("return_type_cd"),
#         col("grantor_business_name"),
#         col("tax_period_begin_dt"),
#         col("tax_period_end_dt"),
#         col("grantor_business_name_control"),
#         coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#         when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#              (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#              when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                   (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                   concat_ws(" ",
#                       col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                       col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                   ))
#              .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#         .otherwise(coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName"))).alias("grantee_business_name"),
#         when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#              concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#         .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#         col("grantee.USAddress.CityNm").alias("grantee_city"),
#         col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#         col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#         col("grantee.RecipientEIN").alias("grantee_ein"),
#         col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#         coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt"), col("grantee.GrantAmt")).alias("grantee_amount"),
#         col("grantee_total_amount_in_year"),
#         col("grantee_501c3_org_count"),
#         col("grantee_other_org_count"),
#         col("grantee_individual_grant_type"),
#         col("grantee_individual_recipient_count"),
#         col("grantee_individual_cash_amount_other"),
#         col("total_volunteers"),
#         col("mission_statement"),
#         lit(None).cast(StringType()).alias("grantee_foundation_status"),
#         lit(None).cast(StringType()).alias("grantee_relationship"),
#         lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#         lit(None).cast(StringType()).alias("grantee_cash_amount"),
#         lit(None).cast(StringType()).alias("grant_paid_year"),
#         lit(None).cast(StringType()).alias("primary_purpose_statement"),
#         lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#         lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#         lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#         lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#         lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#     )
    
#     # Debug: Check grantee_990_df
#     grantee_count = grantee_990_df.count()
#     logger.info(f"Form 990 Grantee rows for {input_path}: {grantee_count}")
#     if grantee_count == 0:
#         logger.warning(f"Empty grantee_990_df for {input_path}")
#         form_990_df.select(
#             "ReturnHeader.Filer.EIN",
#             "ReturnData.IRS990ScheduleI"
#         ).show(1, truncate=False, vertical=True)
#         # Infer schema for IRS990ScheduleI
#         logger.info("Inferring schema for ReturnData.IRS990ScheduleI")
#         temp_df = spark.read \
#             .format("xml") \
#             .option("rowTag", "Return") \
#             .option("wholeFile", "true") \
#             .option("inferSchema", "true") \
#             .load(input_path)
#         temp_df.select("ReturnData.IRS990ScheduleI").printSchema()
#         # Try alternative GrantsAndAllocations
#         if form_990_df.select("ReturnData.IRS990ScheduleI.GrantsAndAllocations").count() > 0:
#             logger.info("Attempting to extract from GrantsAndAllocations")
#             alt_grantee_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990ScheduleI.GrantsAndAllocations")).alias("grantee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 coalesce(col("grantee.RecipientPersonNm"), col("grantee.RecipientName")).alias("grantee_name"),
#                 col("grantee.RecipientBusinessName.BusinessNameLine1Txt").alias("grantee_business_name"),
#                 col("grantee.USAddress.AddressLine1Txt").alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 coalesce(col("grantee.CashGrantAmt"), col("grantee.Amt"), col("grantee.GrantAmt")).alias("grantee_amount")
#             )
#             alt_count = alt_grantee_df.count()
#             logger.info(f"GrantsAndAllocations rows: {alt_count}")
#             if alt_count > 0:
#                 alt_grantee_df.show(5, truncate=False)
#     else:
#         logger.info(f"Form 990 Grantee sample for {input_path}:")
#         grantee_990_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)
    
#     # Write output
#     if grantee_count > 0:
#         try:
#             grantee_990_df.repartition(1).write \
#                 .mode("overwrite") \
#                 .parquet(output_path)
#             logger.info(f"Written grantee Parquet files to {output_path}")
#         except Exception as e:
#             logger.error(f"Error writing grantee to {output_path}: {str(e)}")
#             raise
#     else:
#         logger.warning(f"Skipping write to {output_path} due to empty grantee_990_df")

# except Exception as e:
#     logger.error(f"Error processing {input_path}: {str(e)}")
#     raise
# finally:
#     job.commit()
#     logger.info("ETL job completed successfully!")



#####Working fine code


# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# # Use schema from simplified code
# board_member_990_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("KeyEmployeeInd", StringType(), True),
#     StructField("FormerInd", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# # Minimal schema for Form 990 board members
# form_990_schema = StructType([
#     StructField("ReturnHeader", StructType([
#         StructField("ReturnTypeCd", StringType(), True),
#         StructField("TaxYr", StringType(), True),
#         StructField("Filer", StructType([
#             StructField("EIN", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True)
#     ]), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#             StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_990_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#             StructField("RecipientTable", ArrayType(grantee_schema), True),
#             StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#             StructField("Total501c3OrgCnt", StringType(), True),
#             StructField("TotalOtherOrgCnt", IntegerType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         try:
#             # Read with minimal schema for Form 990 board members
#             form_990_df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(form_990_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name()) \
#                 .filter(col("ReturnHeader.ReturnTypeCd") == "990")
            
#             # Extract object_id
#             form_990_df = form_990_df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
            
#             # Debug: Inspect Form 990 data
#             form_990_count = form_990_df.count()
#             logger.info(f"Form 990 rows in {input_path}: {form_990_count}")
#             if form_990_count > 0:
#                 logger.info("Sample Form 990 data:")
#                 form_990_df.select(
#                     "ReturnHeader.Filer.EIN",
#                     "ReturnHeader.ReturnTypeCd",
#                     "ReturnData.IRS990.Form990PartVIISectionAGrp"
#                 ).show(2, truncate=False, vertical=True)
#                 # Specific check for Foundation for the Carolinas
#                 foundation_df = form_990_df.filter(col("ReturnHeader.Filer.EIN") == "566047886")
#                 if foundation_df.count() > 0:
#                     logger.info("Foundation for the Carolinas (EIN 566047886) found:")
#                     foundation_df.select(
#                         "ReturnHeader.Filer.EIN",
#                         "ReturnData.IRS990.Form990PartVIISectionAGrp"
#                     ).show(1, truncate=False, vertical=True)
#                 else:
#                     logger.warning("Foundation for the Carolinas (EIN 566047886) not found in Form 990 data")
#             else:
#                 logger.warning(f"No Form 990 data in {input_path}")

#             # Form 990 Board Member DataFrame
#             board_member_990_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                 col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 lit(None).cast(StringType()).alias("board_member_address"),
#                 lit(None).cast(StringType()).alias("board_member_city"),
#                 lit(None).cast(StringType()).alias("board_member_state_cd"),
#                 lit(None).cast(StringType()).alias("board_member_zip_code"),
#                 col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                 col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("board_member_highest_compensation"),
#                 lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )
            
#             # Debug: Check board_member_990_df
#             count_990 = board_member_990_df.count()
#             logger.info(f"Form 990 Board Member rows for {input_path}: {count_990}")
#             if count_990 == 0:
#                 logger.warning(f"Empty board_member_990_df for {input_path}")
#                 form_990_df.select(
#                     "ReturnHeader.Filer.EIN",
#                     "ReturnData.IRS990.Form990PartVIISectionAGrp"
#                 ).show(2, truncate=False, vertical=True)
#             else:
#                 logger.info(f"Form 990 Board Member sample for {input_path}:")
#                 board_member_990_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)

#             # Read with full schema for other entities
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with full schema")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             # Form 990EZ Board Member DataFrame
#             board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Form 990PF Board Member DataFrame
#             board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("parent_highest_compensation").alias("board_member_highest_compensation"),
#                 col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
#                 col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
#                 col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Union all board member DataFrames
#             board_member_df = board_member_990_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
#             # Debug: Final board member output
#             total_count = board_member_df.count()
#             logger.info(f"Total Board Member rows for {input_path}: {total_count}")
#             if total_count == 0:
#                 logger.warning(f"Empty final board_member_df for {input_path}")
#             else:
#                 logger.info(f"Final Board Member sample for {input_path}:")
#                 board_member_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year")
#             )
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 grantee_990pf_df.return_type_cd,
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 col("grantee.CashGrantAmt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 col("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise


############# only noard members for the 990 in multiple folders


# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, regexp_extract, substring_index, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled')

# # Initialize Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized")
#     logger.info(f"Spark configs: driver.memory={spark.conf.get('spark.driver.memory', 'default')}")
# except Exception as e:
#     logger.error(f"Job init failed: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"
# bucket_name = "lumino-data"

# # S3 client
# s3_client = boto3.client('s3')

# # Schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("TaxYr", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Verify files exist
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         try:
#             # Read XML
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"XML read from {input_path}")
            
#             # Debug
#             df.printSchema()
#             df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
            
#             form_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990")
#             if form_990_df.rdd.isEmpty():
#                 logger.warning(f"No Form 990 data in {input_path}")
#                 continue
            
#             # Board Member DataFrame
#             board_member_df = form_990_df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                 col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_per_week_related_org"),
#                 col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )
            
#             # Debug
#             count = board_member_df.count()
#             logger.info(f"Board Member rows for {input_path}: {count}")
#             if count == 0:
#                 logger.warning(f"Empty board_member_df for {input_path}")
#                 form_990_df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
#             else:
#                 logger.info(f"Board Member sample for {input_path}:")
#                 board_member_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)
            
#             # Write
#             if not board_member_df.rdd.isEmpty():
#                 output_path = f"{output_base_path}board_member/year={year}/"
#                 board_member_df.repartition(100).write.mode("append").parquet(output_path)
#                 logger.info(f"Written to {output_path}")
            
#         except Exception as e:
#             logger.error(f"Processing error for {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("Cache cleared")

# job.commit()
# logger.info("Job completed")




##################solution with board members but not working


# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# board_member_schema_990 = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema_990), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#                 StructField("Total501c3OrgCnt", StringType(), True),
#                 StructField("TotalOtherOrgCnt", IntegerType(), True)
#             ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         output_path = f"{output_base_path}year={year}/subfolder={subfolder}"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .option("ignoreNamespace", "true") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             # Debug: Show schema and sample for board members
#             df.select("ReturnHeader.ReturnTypeCd", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
#             df.select("ReturnHeader.ReturnTypeCd", "ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp").show(1, truncate=False, vertical=True)
#             df.select("ReturnHeader.ReturnTypeCd", "ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp").show(1, truncate=False, vertical=True)

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Board Member DataFrame
#             # Split by form type to handle schema differences
#             board_member_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#                 col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 lit(None).cast(StringType()).alias("board_member_address"),
#                 lit(None).cast(StringType()).alias("board_member_city"),
#                 lit(None).cast(StringType()).alias("board_member_state_cd"),
#                 lit(None).cast(StringType()).alias("board_member_zip_code"),
#                 col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 lit(None).cast(DoubleType()).alias("board_member_compensation_amt"),
#                 col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("board_member_highest_compensation"),
#                 lit(None).cast(StringType()).alias("board_member_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("board_member_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             board_member_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 lit(None).cast(StringType()).alias("parent_highest_compensation"),
#                 lit(None).cast(StringType()).alias("parent_over_50k_employee_count"),
#                 lit(None).cast(StringType()).alias("parent_highest_paid_contractor"),
#                 lit(None).cast(StringType()).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             board_member_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp")).alias("board_member"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_reportable_comp_org_amt"),
#                 lit(None).cast(DoubleType()).alias("board_member_total_other_compensation_amt"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt").alias("parent_highest_compensation"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt").alias("parent_over_50k_employee_count"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt").alias("parent_highest_paid_contractor"),
#                 col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt").alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 col("board_member.AverageHrsPerWkDevotedToPosRt").alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 col("board_member.CompensationAmt").alias("board_member_reportable_comp_org_amt"),
#                 col("board_member.EmployeeBenefitProgramAmt").alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_expense_or_allowance_amount"),
#                 col("board_member.ExpenseAccountOtherAllwncAmt").alias("board_member_estimated_other_compensation_amt"),
#                 col("parent_highest_compensation").alias("board_member_highest_compensation"),
#                 col("parent_over_50k_employee_count").alias("board_member_over_50k_employee_count"),
#                 col("parent_highest_paid_contractor").alias("board_member_highest_paid_contractor"),
#                 col("parent_contractor_over_50k_count").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.OfficerInd").alias("board_member_officer_ind"),
#                 col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )

#             # Union the board member DataFrames
#             board_member_df = board_member_990_df.union(board_member_990ez_df).union(board_member_990pf_df)
            
#             # Debug: Check board_member_df
#             count = board_member_df.count()
#             logger.info(f"Board Member rows for {input_path}: {count}")
#             if count == 0:
#                 logger.warning(f"Empty board_member_df for {input_path}")
#                 df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
#                 df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp").show(1, truncate=False, vertical=True)
#                 df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp").show(1, truncate=False, vertical=True)
#             else:
#                 logger.info(f"Board Member sample for {input_path}:")
#                 board_member_df.filter(col("grantor_ein") == "566047886").show(5, truncate=False)

#             logger.info("Extracted board_member information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year")
#             )
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 grantee_990pf_df.return_type_cd,
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 col("grantee.CashGrantAmt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 col("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise






#############below code is for foundation for carolinas board members

# import sys
# import logging
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, regexp_extract
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled')

# # Initialize Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized")
#     logger.info(f"Spark configs: driver.memory={spark.conf.get('spark.driver.memory', 'default')}")
# except Exception as e:
#     logger.error(f"Job init failed: {str(e)}")
#     raise

# # S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/2020/download990xml_2020_2/202123199349322547_public.xml"
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"
# bucket_name = "lumino-data"

# # S3 client
# s3_client = boto3.client('s3')

# # Schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True)
#         ]), True)
#     ]), True),
#     StructField("TaxYr", StringType(), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("OfficerInd", StringType(), True),
#     StructField("HighestCompensatedEmployeeInd", StringType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing: {base_input_path}")

# # Verify file
# s3_prefix = base_input_path.replace(f"s3://{bucket_name}/", "")
# try:
#     s3_client.head_object(Bucket=bucket_name, Key=s3_prefix)
#     logger.info(f"File found: s3://{bucket_name}/{s3_prefix}")
# except s3_client.exceptions.ClientError as e:
#     logger.error(f"File error: {str(e)}")
#     raise

# # Read XML
# try:
#     df = spark.read \
#         .format("xml") \
#         .option("rowTag", "Return") \
#         .option("wholeFile", "true") \
#         .option("inferSchema", "false") \
#         .option("ignoreNamespace", "true") \
#         .schema(unified_schema) \
#         .load(base_input_path) \
#         .withColumn("input_file_name", lit(base_input_path))
    
#     if df.rdd.isEmpty():
#         logger.error("Empty DataFrame")
#         raise Exception("Empty DataFrame")
    
#     df = df.withColumn("object_id", regexp_extract(col("input_file_name"), r"(\d+)", 1))
#     logger.info("XML read")
    
#     # Debug
#     df.printSchema()
#     df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
    
#     form_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990")
#     if form_990_df.rdd.isEmpty():
#         logger.warning("No Form 990 data")
#         raise Exception("No Form 990")
    
#     # Board Member DataFrame
#     board_member_df = form_990_df.select(
#         col("object_id"),
#         col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#         col("ReturnHeader.TaxYr").alias("tax_year"),
#         col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#         explode(col("ReturnData.IRS990.Form990PartVIISectionAGrp")).alias("board_member"),
#         col("ReturnData.IRS990.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#         col("ReturnData.IRS990.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#     ).select(
#         col("object_id"),
#         col("grantor_ein"),
#         col("tax_year"),
#         col("return_type_cd"),
#         col("board_member.PersonNm").alias("board_member_person_name"),
#         col("board_member.TitleTxt").alias("board_member_title"),
#         col("board_member.AverageHoursPerWeekRt").alias("board_member_average_hours_per_week"),
#         col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_per_week_related_org"),
#         col("board_member.ReportableCompFromOrgAmt").alias("board_member_reportable_comp_org_amt"),
#         col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#         col("board_member.OtherCompensationAmt").alias("board_member_estimated_other_compensation_amt"),
#         col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#         col("board_member.OfficerInd").alias("board_member_officer_ind"),
#         col("board_member.HighestCompensatedEmployeeInd").alias("board_member_high_comp_employee_ind"),
#         col("board_member_total_reportable_comp_org_amt"),
#         col("board_member_total_other_compensation_amt")
#     )
    
#     # Debug
#     count = board_member_df.count()
#     logger.info(f"Board Member rows: {count}")
#     if count == 0:
#         logger.warning("Empty board_member_df")
#         form_990_df.select("ReturnHeader.Filer.EIN", "ReturnData.IRS990.Form990PartVIISectionAGrp").show(1, truncate=False, vertical=True)
#     else:
#         logger.info("Board Member sample:")
#         board_member_df.show(5, truncate=False)
    
#     # Write
#     if not board_member_df.rdd.isEmpty():
#         output_path = f"{output_base_path}board_member/year=2020/"
#         board_member_df.repartition(1).write.mode("append").parquet(output_path)
#         logger.info(f"Written to {output_path}")
    
# except Exception as e:
#     logger.error(f"Processing error: {str(e)}")
#     raise
# finally:
#     df.unpersist()
#     logger.info("Cache cleared")

# job.commit()
# logger.info("Job completed")


######################################## below is godd code and functioning without 990 grantee and boardmembers(foundation for carolinas)



# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
    
#     # Spark configurations
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info("Spark configurations set successfully")
    
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("ContributorBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True),
#                 StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#                 StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#                 StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#                 StructField("ContractorPaidOver50kCnt", StringType(), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True),
#                 StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True),
#             StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#             StructField("TotalOtherCompensationAmt", DoubleType(), True)
#         ]), True),
#         StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True),
#                 StructField("Total501c3OrgCnt", StringType(), True),
#                 StructField("TotalOtherOrgCnt", IntegerType(), True)
#             ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         output_path = f"{output_base_path}year={year}/subfolder={subfolder}"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Board Member DataFrame
#             board_member_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(
#                     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                          col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                           col("ReturnData.IRS990.Form990PartVIISectionAGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                           col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp"))
#                     .otherwise(lit([]))
#                 ).alias("board_member"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990",
#                      col("ReturnData.IRS990.TotalReportableCompFromOrgAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_total_reportable_comp_org_amt"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990",
#                      col("ReturnData.IRS990.TotalOtherCompensationAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_total_other_compensation_amt"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdEmplOrNONETxt")).otherwise(lit(None).cast(StringType())).alias("parent_highest_compensation"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OtherEmployeePaidOver50kCnt")).otherwise(lit(None).cast(StringType())).alias("parent_over_50k_employee_count"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompOfHghstPdCntrctOrNONETxt")).otherwise(lit(None).cast(StringType())).alias("parent_highest_paid_contractor"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.ContractorPaidOver50kCnt")).otherwise(lit(None).cast(StringType())).alias("parent_contractor_over_50k_count")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.AverageHrsPerWkDevotedToPosRt"))
#                 .when(col("return_type_cd") == "990",
#                       col("board_member.AverageHoursPerWeekRt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.ReportableCompFromOrgAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.CompensationAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_reportable_comp_org_amt"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.EmployeeBenefitProgramAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("board_member.ExpenseAccountOtherAllwncAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_expense_or_allowance_amount"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.OtherCompensationAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.ExpenseAccountOtherAllwncAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_estimated_other_compensation_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("parent_highest_compensation"))
#                 .otherwise(col("board_member.CompOfHghstPdEmplOrNONETxt")).alias("board_member_highest_compensation"),
#                 when(col("return_type_cd") == "990PF",
#                      col("parent_over_50k_employee_count"))
#                 .otherwise(col("board_member.OtherEmployeePaidOver50kCnt")).alias("board_member_over_50k_employee_count"),
#                 when(col("return_type_cd") == "990PF",
#                      col("parent_highest_paid_contractor"))
#                 .otherwise(col("board_member.CompOfHghstPdCntrctOrNONETxt")).alias("board_member_highest_paid_contractor"),
#                 when(col("return_type_cd") == "990PF",
#                      col("parent_contractor_over_50k_count"))
#                 .otherwise(col("board_member.ContractorPaidOver50kCnt")).alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member_total_reportable_comp_org_amt"),
#                 col("board_member_total_other_compensation_amt")
#             )
#             logger.info("Extracted board_member information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year")
#             )
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 grantee_990pf_df.return_type_cd,
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990ScheduleI.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("ReturnData.IRS990ScheduleI.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 col("grantee.CashGrantAmt").alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 coalesce(
#                     col("contributor.ContributorPersonNm"),
#                     when((col("contributor.ContributorBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                          (col("contributor.ContributorBusinessName.BusinessNameLine2Txt") != ""),
#                          concat_ws(" ",
#                              col("contributor.ContributorBusinessName.BusinessNameLine1Txt"),
#                              col("contributor.ContributorBusinessName.BusinessNameLine2Txt")
#                          ))
#                     .otherwise(col("contributor.ContributorBusinessName.BusinessNameLine1Txt"))
#                 ).alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise



###################below is the original code

# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
    
#     # Spark configurations
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info("Spark configurations set successfully")
    
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_extraction_test1/xml_raw/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
#     # "2019": ["2019_TEOS_XML_CT1", "download990xml_2019_1", "download990xml_2019_2", "download990xml_2019_3", "download990xml_2019_4", "download990xml_2019_5", "download990xml_2019_6", "download990xml_2019_7", "download990xml_2019_8"],
#     # "2020": ["2020_TEOS_XML_CT1", "2020_download990xml_2020_1", "download990xml_2020_2", "download990xml_2020_3", "download990xml_2020_4", "2020_download990xml_2020_5", "download990xml_2020_6", "download990xml_2020_7", "download990xml_2020_8"],
#     # "2021": ["2021Redo_allCycles"],
#     # "2022": ["2022Redo_cycle01_41"],
#     # "2023": ["2023_TEOS_XML_01A", "2023_TEOS_XML_02A", "2023_TEOS_XML_03A", "2023_TEOS_XML_04A", "2023_TEOS_XML_05A", "2023_TEOS_XML_06A", "2023_TEOS_XML_07A", "2023_TEOS_XML_08A", "2023_TEOS_XML_09A", "2023_TEOS_XML_10A", "2023_TEOS_XML_11A", "2023_TEOS_XML_12A"],
#     # "2024": ["2024_TEOS_XML_01A", "2024_TEOS_XML_02A", "2024_TEOS_XML_03A", "2024_TEOS_XML_04A", "2024_TEOS_XML_05A", "2024_TEOS_XML_06A", "2024_TEOS_XML_07A", "2024_TEOS_XML_08A", "2024_TEOS_XML_09A", "2024_TEOS_XML_10A", "2024_TEOS_XML_11A", "2024_TEOS_XML_12A"]
# }
# output_base_path = "s3://lumino-data/xml_extraction_test1/xml_processed/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("TotalOtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Total501c3OrgCnt", StringType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("BusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("TotalOtherOrgCnt", IntegerType(), True),
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True)
#             ]), True),
#             StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         output_path = f"{output_base_path}year={year}/subfolder={subfolder}"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_raw/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)  # Limit to 1 row to check existence without loading all data

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()  # Clean up the small sample DataFrame
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id by taking the filename and matching the numerical part
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)  # Get the last part (e.g., 202122519349100407_public.xml)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)  # Extract the numerical part (e.g., 202122519349100407)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             # Check for empty DataFrame
#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             board_member_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(
#                     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                          col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                           col("ReturnData.IRS990.Form990PartVIISectionAGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                           col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp"))
#                     .otherwise(lit([]))
#                 ).alias("board_member")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.AverageHrsPerWkDevotedToPosRt"))
#                 .when(col("return_type_cd") == "990",
#                       col("board_member.AverageHoursPerWeekRt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.ReportableCompFromOrgAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.CompensationAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_reportable_comp_org_amt"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.EmployeeBenefitProgramAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("board_member.ExpenseAccountOtherAllwncAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_expense_or_allowance_amount"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.OtherCompensationAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.ExpenseAccountOtherAllwncAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("board_member.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#             )
#             logger.info("Extracted board_member information")

#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ContributingManagerNm").alias("grantee_contributing_manager_name"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ShareholderManagerNm").alias("grantee_shareholder_manager_name")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                          col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_contributing_manager_name"),
#                 col("grantee_shareholder_manager_name"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement")
#             )

#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990PF.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 when((col("contributor.ContributorPersonNm").isNotNull()) & (col("contributor.ContributorPersonNm") != ""),
#                      col("contributor.ContributorPersonNm"))
#                 .when((col("contributor.BusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                       (col("contributor.BusinessName.BusinessNameLine2Txt") != ""),
#                       concat_ws(" ",
#                           col("contributor.BusinessName.BusinessNameLine1Txt"),
#                           col("contributor.BusinessName.BusinessNameLine2Txt")
#                       ))
#                 .otherwise(col("contributor.BusinessName.BusinessNameLine1Txt"))
#                 .alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                          col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year")
#             )

#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_preselected_indicator"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year")
#             )

#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             combined_df = grantor_df.join(board_member_df, ["object_id", "grantor_ein"], "left_outer") \
#                                   .join(grantee_df, ["object_id", "grantor_ein"], "left_outer") \
#                                   .join(contact_df, ["object_id", "grantor_ein"], "left_outer") \
#                                   .join(employee_990pf_df, ["object_id", "grantor_ein"], "left_outer") \
#                                   .join(contributor_990pf_df, ["object_id", "grantor_ein"], "left_outer")

#             expected_fields = [
#                 "object_id", "return_type_cd", "tax_period_begin_dt", "tax_period_end_dt", "tax_year", "grantor_ein",
#                 "grantor_business_name", "grantor_business_name_control", "grantor_phone_num", "grantor_address",
#                 "grantor_city", "grantor_state_cd", "grantor_zip_code", "board_member_person_name", "board_member_title",
#                 "board_member_address", "board_member_city", "board_member_state_cd", "board_member_zip_code",
#                 "board_member_average_hours_per_week", "board_member_average_hours_related_org",
#                 "board_member_compensation_amt", "board_member_reportable_comp_org_amt",
#                 "board_member_employee_benefit_amount", "board_member_reportable_comp_related_org_amt",
#                 "board_member_expense_or_allowance_amount", "board_member_estimated_other_compensation_amt",
#                 "board_member_highest_compensation", "board_member_over_50k_employee_count",
#                 "board_member_highest_paid_contractor", "board_member_over_50k_contractor_count",
#                 "board_member_trustee_director_ind", "board_member_total_reportable_comp_org_amt",
#                 "board_member_total_other_compensation_amt", "grantee_contributing_manager_name",
#                 "grantee_shareholder_manager_name", "grantee_preselected_indicator", "grantee_business_name",
#                 "grantee_address", "grantee_city", "grantee_state_cd", "grantee_zip_code", "grantee_foundation_status",
#                 "grantee_purpose", "grantee_amount", "grantee_relationship", "grantee_total_amount_in_year",
#                 "grantee_ein", "grantee_irc_section_desc", "grantee_501c3_org_count", "grantee_other_org_count",
#                 "grantee_individual_grant_type", "grantee_individual_recipient_count", "grantee_individual_cash_amount_other",
#                 "grantee_cash_amount", "contact_person_name_for_grants", "contact_address_for_grants",
#                 "contact_city_for_grants", "contact_state_cd_for_grants", "contact_zip_code_for_grants",
#                 "contact_phone_num_for_grants", "contact_form_info_for_grants", "contact_submission_deadlines_for_grants",
#                 "contact_restrictions_for_grants", "total_volunteers", "mission_statement",
#                 "federal_campaign_contributions", "membership_dues", "fundraising_contributions",
#                 "related_organization_support", "government_grants", "other_contributions", "primary_purpose_statement",
#                 "grant_paid_year", "employee_name", "employee_address", "employee_city", "employee_state",
#                 "employee_zip_code", "employee_title", "employee_avg_hours_per_week", "employee_compensation",
#                 "employee_benefit", "employee_expense_allowances",
#                 "contributor_name", "contributor_street_address", "contributor_city", "contributor_state",
#                 "contributor_zip_code", "total_contributed_by_contributor", "person_contribution_ind",
#                 "payroll_contribution_ind", "noncash_contribution_ind", "website"
#             ]

#             for field in expected_fields:
#                 if field not in combined_df.columns:
#                     combined_df = combined_df.withColumn(field, lit(None).cast(
#                         StringType() if field not in [
#                             "board_member_average_hours_per_week", "board_member_average_hours_related_org",
#                             "board_member_compensation_amt", "board_member_reportable_comp_org_amt",
#                             "board_member_employee_benefit_amount", "board_member_reportable_comp_related_org_amt",
#                             "board_member_expense_or_allowance_amount", "board_member_estimated_other_compensation_amt",
#                             "board_member_total_reportable_comp_org_amt", "board_member_total_other_compensation_amt",
#                             "grantee_amount", "grantee_total_amount_in_year", "grantee_individual_cash_amount_other",
#                             "grantee_cash_amount", "federal_campaign_contributions", "membership_dues",
#                             "fundraising_contributions", "related_organization_support", "government_grants",
#                             "other_contributions", "employee_avg_hours_per_week", "employee_compensation",
#                             "employee_benefit", "employee_expense_allowances", "total_contributed_by_contributor"
#                         ] else DoubleType()
#                     ))
#             logger.info("Combined DataFrames and enforced updated schema")

#             # Repartition for better file size management
#             combined_df = combined_df.repartition(100)
#             combined_df.write \
#                 .mode("overwrite") \
#                 .partitionBy("return_type_cd") \
#                 .parquet(output_path)
#             logger.info(f"Written Parquet files to {output_path}")
#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise









##########################

# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
    
#     # Spark configurations
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info("Spark configurations set successfully")
    
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_extraction_test1/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# # output_base_path = "s3://lumino-data/xml_extraction_test1/xml_processed/"
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("TotalOtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Total501c3OrgCnt", StringType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("BusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("TotalOtherOrgCnt", IntegerType(), True),
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True)
#             ]), True),
#             StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         output_path = f"{output_base_path}year={year}/subfolder={subfolder}"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_raw/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)  # Limit to 1 row to check existence without loading all data

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()  # Clean up the small sample DataFrame
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id by taking the filename and matching the numerical part
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)  # Get the last part (e.g., 202122519349100407_public.xml)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)  # Extract the numerical part (e.g., 202122519349100407)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             # Check for empty DataFrame
#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Board Member DataFrame
#             board_member_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(
#                     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                          col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                           col("ReturnData.IRS990.Form990PartVIISectionAGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                           col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp"))
#                     .otherwise(lit([]))
#                 ).alias("board_member")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.AverageHrsPerWkDevotedToPosRt"))
#                 .when(col("return_type_cd") == "990",
#                       col("board_member.AverageHoursPerWeekRt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.ReportableCompFromOrgAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.CompensationAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_reportable_comp_org_amt"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.EmployeeBenefitProgramAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("board_member.ExpenseAccountOtherAllwncAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_expense_or_allowance_amount"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.OtherCompensationAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.ExpenseAccountOtherAllwncAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("board_member.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#             )
#             logger.info("Extracted board_member information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ContributingManagerNm").alias("grantee_contributing_manager_name"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ShareholderManagerNm").alias("grantee_shareholder_manager_name")
#             )
#             # Join with contact_df to add contact columns
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                          col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_contributing_manager_name"),
#                 col("grantee_shareholder_manager_name"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                          col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 col("grantee.PurposeOfGrantTxt").alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_preselected_indicator"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 when((col("contributor.ContributorPersonNm").isNotNull()) & (col("contributor.ContributorPersonNm") != ""),
#                      col("contributor.ContributorPersonNm"))
#                 .when((col("contributor.BusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                       (col("contributor.BusinessName.BusinessNameLine2Txt") != ""),
#                       concat_ws(" ",
#                           col("contributor.BusinessName.BusinessNameLine1Txt"),
#                           col("contributor.BusinessName.BusinessNameLine2Txt")
#                       ))
#                 .otherwise(col("contributor.BusinessName.BusinessNameLine1Txt"))
#                 .alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 # Check if DataFrame is empty
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 # Ensure required columns are present
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 # Define output path for this entity
#                 entity_output_path = f"{output_path}/entity={entity_name}"
                
#                 # Write DataFrame to Parquet
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("overwrite") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise





########################################### Working fine but improper structure



# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
    
#     # Spark configurations
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info("Spark configurations set successfully")
    
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("TotalOtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Total501c3OrgCnt", StringType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("BusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("TotalOtherOrgCnt", IntegerType(), True),
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True)
#             ]), True),
#             StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         output_path = f"{output_base_path}year={year}/subfolder={subfolder}"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         # s3://lumino-data/xml_data_extraction/xml_test/
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)  # Limit to 1 row to check existence without loading all data

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()  # Clean up the small sample DataFrame
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id by taking the filename and matching the numerical part
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)  # Get the last part (e.g., 202122519349100407_public.xml)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)  # Extract the numerical part (e.g., 202122519349100407)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             # Check for empty DataFrame
#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Board Member DataFrame
#             board_member_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(
#                     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                          col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                           col("ReturnData.IRS990.Form990PartVIISectionAGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                           col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp"))
#                     .otherwise(lit([]))
#                 ).alias("board_member")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.AverageHrsPerWkDevotedToPosRt"))
#                 .when(col("return_type_cd") == "990",
#                       col("board_member.AverageHoursPerWeekRt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.ReportableCompFromOrgAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.CompensationAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_reportable_comp_org_amt"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.EmployeeBenefitProgramAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("board_member.ExpenseAccountOtherAllwncAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_expense_or_allowance_amount"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.OtherCompensationAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.ExpenseAccountOtherAllwncAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("board_member.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#             )
#             logger.info("Extracted board_member information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ContributingManagerNm").alias("grantee_contributing_manager_name"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ShareholderManagerNm").alias("grantee_shareholder_manager_name")
#             )
#             # Join with contact_df to add contact columns
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 grantee_990pf_df.return_type_cd,  # Disambiguate return_type_cd
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_contributing_manager_name"),
#                 col("grantee_shareholder_manager_name"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_preselected_indicator"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 when((col("contributor.ContributorPersonNm").isNotNull()) & (col("contributor.ContributorPersonNm") != ""),
#                      col("contributor.ContributorPersonNm"))
#                 .when((col("contributor.BusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                       (col("contributor.BusinessName.BusinessNameLine2Txt") != ""),
#                       concat_ws(" ",
#                           col("contributor.BusinessName.BusinessNameLine1Txt"),
#                           col("contributor.BusinessName.BusinessNameLine2Txt")
#                       ))
#                 .otherwise(col("contributor.BusinessName.BusinessNameLine1Txt"))
#                 .alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 # Check if DataFrame is empty
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 # Ensure required columns are present
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 # Define output path for this entity
#                 entity_output_path = f"{output_path}/entity={entity_name}"
                
#                 # Write DataFrame to Parquet
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("overwrite") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise



###################proper structure

# import sys
# import logging
# import time
# import boto3
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from pyspark.sql.functions import col, lit, explode, when, concat_ws, substring_index, regexp_extract, coalesce, input_file_name
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# # Configure logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# logger.addHandler(handler)
# logger.info('Logger enabled and integrated with CloudWatch Logs')

# # Initialize AWS Glue job
# try:
#     args = getResolvedOptions(sys.argv, ["JOB_NAME"])
#     sc = SparkContext()
#     glueContext = GlueContext(sc)
#     spark = glueContext.spark_session
#     job = Job(glueContext)
#     job.init(args["JOB_NAME"], args)
#     logger.info(f"Job {args['JOB_NAME']} initialized successfully")
    
#     # Spark configurations
#     spark.conf.set("spark.sql.shuffle.partitions", "100")
#     spark.conf.set("spark.default.parallelism", "100")
#     logger.info("Spark configurations set successfully")
    
# except Exception as e:
#     logger.error(f"Failed to initialize Glue job: {str(e)}")
#     raise

# # Define S3 paths
# base_input_path = "s3://lumino-data/xml_data_extraction/xml_test/"
# years = ["2019", "2020"]
# subfolders = {
#     "2019": ["download990xml_2019_1", "download990xml_2019_2"],
#     "2020": ["download990xml_2020_2", "download990xml_2020_3"]
# }
# output_base_path = "s3://lumino-data/xml_data_extraction/xml_test_extract/"

# # Initialize S3 client to check for files
# s3_client = boto3.client('s3')
# bucket_name = "lumino-data"

# # Define schemas
# return_header_schema = StructType([
#     StructField("ReturnTypeCd", StringType(), True),
#     StructField("TaxPeriodBeginDt", StringType(), True),
#     StructField("TaxPeriodEndDt", StringType(), True),
#     StructField("TaxYr", StringType(), True),
#     StructField("Filer", StructType([
#         StructField("EIN", StringType(), True),
#         StructField("BusinessName", StructType([
#             StructField("BusinessNameLine1Txt", StringType(), True),
#             StructField("BusinessNameLine2Txt", StringType(), True)
#         ]), True),
#         StructField("BusinessNameControlTxt", StringType(), True),
#         StructField("PhoneNum", StringType(), True),
#         StructField("USAddress", StructType([
#             StructField("AddressLine1Txt", StringType(), True),
#             StructField("AddressLine2Txt", StringType(), True),
#             StructField("CityNm", StringType(), True),
#             StructField("StateAbbreviationCd", StringType(), True),
#             StructField("ZIPCd", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# board_member_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRt", DoubleType(), True),
#     StructField("AverageHoursPerWeekRltdOrgRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitProgramAmt", DoubleType(), True),
#     StructField("ExpenseAccountOtherAllwncAmt", DoubleType(), True),
#     StructField("CompOfHghstPdEmplOrNONETxt", StringType(), True),
#     StructField("OtherEmployeePaidOver50kCnt", StringType(), True),
#     StructField("CompOfHghstPdCntrctOrNONETxt", StringType(), True),
#     StructField("ContractorPaidOver50kCnt", StringType(), True),
#     StructField("IndividualTrusteeOrDirectorInd", StringType(), True),
#     StructField("ReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("ReportableCompFromRltdOrgAmt", DoubleType(), True),
#     StructField("OtherCompensationAmt", DoubleType(), True),
#     StructField("TotalReportableCompFromOrgAmt", DoubleType(), True),
#     StructField("TotalOtherCompensationAmt", DoubleType(), True)
# ])

# employee_schema = StructType([
#     StructField("PersonNm", StringType(), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TitleTxt", StringType(), True),
#     StructField("AverageHrsPerWkDevotedToPosRt", DoubleType(), True),
#     StructField("CompensationAmt", DoubleType(), True),
#     StructField("EmployeeBenefitsAmt", DoubleType(), True),
#     StructField("ExpenseAccountAmt", DoubleType(), True)
# ])

# grantee_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientBusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("USAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientFoundationStatusTxt", StringType(), True),
#     StructField("GrantOrContributionPurposeTxt", StringType(), True),
#     StructField("PurposeOfGrantTxt", StringType(), True),
#     StructField("Amt", DoubleType(), True),
#     StructField("TotalGrantOrContriPdDurYrAmt", DoubleType(), True),
#     StructField("OnlyContriToPreselectedInd", StringType(), True),
#     StructField("RecipientRelationshipTxt", StringType(), True),
#     StructField("RecipientEIN", StringType(), True),
#     StructField("IRCSectionDesc", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True),
#     StructField("Total501c3OrgCnt", StringType(), True),
#     StructField("TaxYr", StringType(), True)
# ])

# contributor_schema = StructType([
#     StructField("ContributorPersonNm", StringType(), True),
#     StructField("BusinessName", StructType([
#         StructField("BusinessNameLine1Txt", StringType(), True),
#         StructField("BusinessNameLine2Txt", StringType(), True)
#     ]), True),
#     StructField("ContributorUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("TotalContributionsAmt", DoubleType(), True),
#     StructField("PersonContributionInd", StringType(), True),
#     StructField("PayrollContributionInd", StringType(), True),
#     StructField("NoncashContributionInd", StringType(), True)
# ])

# contact_schema = StructType([
#     StructField("RecipientPersonNm", StringType(), True),
#     StructField("RecipientUSAddress", StructType([
#         StructField("AddressLine1Txt", StringType(), True),
#         StructField("AddressLine2Txt", StringType(), True),
#         StructField("CityNm", StringType(), True),
#         StructField("StateAbbreviationCd", StringType(), True),
#         StructField("ZIPCd", StringType(), True)
#     ]), True),
#     StructField("RecipientPhoneNum", StringType(), True),
#     StructField("FormAndInfoAndMaterialsTxt", StringType(), True),
#     StructField("SubmissionDeadlinesTxt", StringType(), True),
#     StructField("RestrictionsOnAwardsTxt", StringType(), True)
# ])

# grants_other_assist_schema = StructType([
#     StructField("TotalOtherOrgCnt", IntegerType(), True),
#     StructField("GrantTypeTxt", StringType(), True),
#     StructField("RecipientCnt", StringType(), True),
#     StructField("CashGrantAmt", DoubleType(), True)
# ])

# unified_schema = StructType([
#     StructField("ReturnHeader", return_header_schema, True),
#     StructField("ReturnData", StructType([
#         StructField("IRS990PF", StructType([
#             StructField("OfficerDirTrstKeyEmplInfoGrp", StructType([
#                 StructField("OfficerDirTrstKeyEmplGrp", ArrayType(board_member_schema), True),
#                 StructField("CompensationHighestPaidEmplGrp", ArrayType(employee_schema), True)
#             ]), True),
#             StructField("SupplementaryInformationGrp", StructType([
#                 StructField("ContributingManagerNm", StringType(), True),
#                 StructField("ShareholderManagerNm", StringType(), True),
#                 StructField("GrantOrContributionPdDurYrGrp", ArrayType(grantee_schema), True),
#                 StructField("ApplicationSubmissionInfoGrp", ArrayType(contact_schema), True)
#             ]), True),
#             StructField("IRS990ScheduleB", StructType([
#                 StructField("ContributorInformationGrp", ArrayType(contributor_schema), True)
#             ]), True),
#             StructField("StatementsRegardingActyGrp", StructType([
#                 StructField("WebsiteAddressTxt", StringType(), True)
#             ]), True)
#         ]), True),
#         StructField("IRS990", StructType([
#             StructField("Form990PartVIISectionAGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsToDomesticOrgsGrp", StructType([
#                 StructField("TotalAmt", DoubleType(), True)
#             ]), True),
#             StructField("IRS990ScheduleI", StructType([
#                 StructField("RecipientTable", ArrayType(grantee_schema), True),
#                 StructField("GrantsOtherAsstToIndivInUSGrp", grants_other_assist_schema, True)
#             ]), True),
#             StructField("TotalVolunteersCnt", StringType(), True),
#             StructField("ActivityOrMissionDesc", StringType(), True),
#             StructField("FederatedCampaignsAmt", DoubleType(), True),
#             StructField("MembershipDuesAmt", DoubleType(), True),
#             StructField("FundraisingAmt", DoubleType(), True),
#             StructField("RelatedOrganizationsAmt", DoubleType(), True),
#             StructField("GovernmentGrantsAmt", DoubleType(), True),
#             StructField("AllOtherContributionsAmt", DoubleType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True),
#         StructField("IRS990EZ", StructType([
#             StructField("OfficerDirectorTrusteeEmplGrp", ArrayType(board_member_schema), True),
#             StructField("GrantsAndSimilarAmountsPaidAmt", DoubleType(), True),
#             StructField("PrimaryExemptPurposeTxt", StringType(), True),
#             StructField("WebsiteAddressTxt", StringType(), True)
#         ]), True)
#     ]), True)
# ])

# logger.info(f"Processing XML files from {base_input_path}")

# for year in years:
#     for subfolder in subfolders[year]:
#         input_path = f"{base_input_path}{year}/{subfolder}/*.xml"
#         logger.info(f"Processing XML files from {input_path}")
        
#         # Check if files exist in the S3 path
#         s3_prefix = f"xml_data_extraction/xml_test/{year}/{subfolder}/"
#         logger.info(f"Checking for files in s3://{bucket_name}/{s3_prefix}")
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

#         if 'Contents' not in response or len(response['Contents']) == 0:
#             logger.warning(f"No files found in s3://{bucket_name}/{s3_prefix}. Skipping folder.")
#             continue

#         # If files exist, proceed with processing
#         try:
#             # Attempt to read a small sample to check file existence
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .limit(1)  # Limit to 1 row to check existence without loading all data

#             if df.rdd.isEmpty():
#                 logger.warning(f"No files found in {input_path}, skipping")
#                 continue
#             else:
#                 df.unpersist()  # Clean up the small sample DataFrame
#                 logger.info(f"Files found in {input_path}, proceeding with processing")

#             # Reload the full DataFrame with input_file_name
#             df = spark.read \
#                 .format("xml") \
#                 .option("rowTag", "Return") \
#                 .option("wholeFile", "true") \
#                 .option("inferSchema", "false") \
#                 .schema(unified_schema) \
#                 .load(input_path) \
#                 .withColumn("input_file_name", input_file_name())
            
#             # Extract object_id by taking the filename and matching the numerical part
#             df = df.withColumn(
#                 "filename",
#                 substring_index(col("input_file_name"), "/", -1)  # Get the last part (e.g., 202122519349100407_public.xml)
#             ).withColumn(
#                 "object_id",
#                 regexp_extract(col("filename"), r"(\d+)", 1)  # Extract the numerical part (e.g., 202122519349100407)
#             ).drop("filename")
#             logger.info(f"Successfully read XML files from {input_path} with predefined schema and extracted object_id")
            
#             df.cache()
#             logger.info("DataFrame cached for processing")

#             # Check for empty DataFrame
#             if not df.take(1):
#                 logger.warning(f"No 990, 990EZ, or 990PF files found in {input_path}, skipping")
#                 df.unpersist()
#                 continue
#             logger.info("DataFrame contains data, proceeding with processing")

#             df = df.filter(col("ReturnHeader.ReturnTypeCd").isin("990", "990EZ", "990PF"))

#             invalid_rows = df.filter((col("ReturnHeader.ReturnTypeCd").isNull()) | (col("ReturnHeader.Filer.EIN").isNull()))
#             invalid_sample = invalid_rows.take(1)
#             if invalid_sample:
#                 logger.warning(f"Found invalid rows with missing ReturnHeader.ReturnTypeCd or ReturnHeader.Filer.EIN in {year}/{subfolder}. Sample: {invalid_sample}")
#             df = df.filter((col("ReturnHeader.ReturnTypeCd").isNotNull()) & (col("ReturnHeader.Filer.EIN").isNotNull()))
#             logger.info("Removed invalid rows")

#             # Grantor DataFrame
#             grantor_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnHeader.Filer.PhoneNum").alias("grantor_phone_num"),
#                 col("ReturnHeader.Filer.USAddress.AddressLine1Txt").alias("grantor_address"),
#                 col("ReturnHeader.Filer.USAddress.CityNm").alias("grantor_city"),
#                 col("ReturnHeader.Filer.USAddress.StateAbbreviationCd").alias("grantor_state_cd"),
#                 col("ReturnHeader.Filer.USAddress.ZIPCd").alias("grantor_zip_code"),
#                 when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                      col("ReturnData.IRS990PF.StatementsRegardingActyGrp.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                       col("ReturnData.IRS990.WebsiteAddressTxt"))
#                 .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                       col("ReturnData.IRS990EZ.WebsiteAddressTxt"))
#                 .otherwise(lit(None).cast(StringType())).alias("website")
#             ).na.fill({"grantor_business_name": "", "grantor_address": ""})
#             logger.info("Extracted grantor information")

#             # Board Member DataFrame
#             board_member_df = df.select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 explode(
#                     when(col("ReturnHeader.ReturnTypeCd") == "990PF",
#                          col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.OfficerDirTrstKeyEmplGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990",
#                           col("ReturnData.IRS990.Form990PartVIISectionAGrp"))
#                     .when(col("ReturnHeader.ReturnTypeCd") == "990EZ",
#                           col("ReturnData.IRS990EZ.OfficerDirectorTrusteeEmplGrp"))
#                     .otherwise(lit([]))
#                 ).alias("board_member")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("board_member.PersonNm").alias("board_member_person_name"),
#                 col("board_member.TitleTxt").alias("board_member_title"),
#                 when((col("board_member.USAddress.AddressLine2Txt").isNotNull()) & (col("board_member.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("board_member.USAddress.AddressLine1Txt"), col("board_member.USAddress.AddressLine2Txt")))
#                 .otherwise(col("board_member.USAddress.AddressLine1Txt")).alias("board_member_address"),
#                 col("board_member.USAddress.CityNm").alias("board_member_city"),
#                 col("board_member.USAddress.StateAbbreviationCd").alias("board_member_state_cd"),
#                 col("board_member.USAddress.ZIPCd").alias("board_member_zip_code"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.AverageHrsPerWkDevotedToPosRt"))
#                 .when(col("return_type_cd") == "990",
#                       col("board_member.AverageHoursPerWeekRt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_average_hours_per_week"),
#                 col("board_member.AverageHoursPerWeekRltdOrgRt").alias("board_member_average_hours_related_org"),
#                 col("board_member.CompensationAmt").alias("board_member_compensation_amt"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.ReportableCompFromOrgAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.CompensationAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_reportable_comp_org_amt"),
#                 when(col("return_type_cd").isin("990PF", "990EZ"),
#                      col("board_member.EmployeeBenefitProgramAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_employee_benefit_amount"),
#                 col("board_member.ReportableCompFromRltdOrgAmt").alias("board_member_reportable_comp_related_org_amt"),
#                 when(col("return_type_cd") == "990PF",
#                      col("board_member.ExpenseAccountOtherAllwncAmt")).otherwise(lit(None).cast(DoubleType())).alias("board_member_expense_or_allowance_amount"),
#                 when(col("return_type_cd") == "990",
#                      col("board_member.OtherCompensationAmt"))
#                 .when(col("return_type_cd") == "990EZ",
#                       col("board_member.ExpenseAccountOtherAllwncAmt"))
#                 .otherwise(lit(None).cast(DoubleType())).alias("board_member_estimated_other_compensation_amt"),
#                 col("board_member.CompOfHghstPdEmplOrNONETxt").alias("board_member_highest_compensation"),
#                 col("board_member.OtherEmployeePaidOver50kCnt").alias("board_member_over_50k_employee_count"),
#                 col("board_member.CompOfHghstPdCntrctOrNONETxt").alias("board_member_highest_paid_contractor"),
#                 col("board_member.ContractorPaidOver50kCnt").alias("board_member_over_50k_contractor_count"),
#                 col("board_member.IndividualTrusteeOrDirectorInd").alias("board_member_trustee_director_ind"),
#                 col("board_member.TotalReportableCompFromOrgAmt").alias("board_member_total_reportable_comp_org_amt"),
#                 col("board_member.TotalOtherCompensationAmt").alias("board_member_total_other_compensation_amt")
#             )
#             logger.info("Extracted board_member information")

#             # Employee DataFrame (990PF only)
#             employee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.OfficerDirTrstKeyEmplInfoGrp.CompensationHighestPaidEmplGrp")).alias("employee")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("employee.PersonNm").alias("employee_name"),
#                 when((col("employee.USAddress.AddressLine2Txt").isNotNull()) & (col("employee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("employee.USAddress.AddressLine1Txt"), col("employee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("employee.USAddress.AddressLine1Txt")).alias("employee_address"),
#                 col("employee.USAddress.CityNm").alias("employee_city"),
#                 col("employee.USAddress.StateAbbreviationCd").alias("employee_state"),
#                 col("employee.USAddress.ZIPCd").alias("employee_zip_code"),
#                 col("employee.TitleTxt").alias("employee_title"),
#                 col("employee.AverageHrsPerWkDevotedToPosRt").alias("employee_avg_hours_per_week"),
#                 col("employee.CompensationAmt").alias("employee_compensation"),
#                 col("employee.EmployeeBenefitsAmt").alias("employee_benefit"),
#                 col("employee.ExpenseAccountAmt").alias("employee_expense_allowances")
#             )
#             logger.info("Extracted employee information for 990PF")

#             # Contact DataFrame (990PF only)
#             contact_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.ApplicationSubmissionInfoGrp")).alias("contact")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("contact.RecipientPersonNm").alias("contact_person_name_for_grants"),
#                 when((col("contact.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("contact.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("contact.RecipientUSAddress.AddressLine1Txt"), col("contact.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("contact.RecipientUSAddress.AddressLine1Txt")).alias("contact_address_for_grants"),
#                 col("contact.RecipientUSAddress.CityNm").alias("contact_city_for_grants"),
#                 col("contact.RecipientUSAddress.StateAbbreviationCd").alias("contact_state_cd_for_grants"),
#                 col("contact.RecipientUSAddress.ZIPCd").alias("contact_zip_code_for_grants"),
#                 col("contact.RecipientPhoneNum").alias("contact_phone_num_for_grants"),
#                 col("contact.FormAndInfoAndMaterialsTxt").alias("contact_form_info_for_grants"),
#                 col("contact.SubmissionDeadlinesTxt").alias("contact_submission_deadlines_for_grants"),
#                 col("contact.RestrictionsOnAwardsTxt").alias("contact_restrictions_for_grants")
#             )
#             logger.info("Extracted grantee contact information")

#             # Grantee DataFrame (990PF)
#             grantee_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990PF.SupplementaryInformationGrp.GrantOrContributionPdDurYrGrp")).alias("grantee"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ContributingManagerNm").alias("grantee_contributing_manager_name"),
#                 col("ReturnData.IRS990PF.SupplementaryInformationGrp.ShareholderManagerNm").alias("grantee_shareholder_manager_name")
#             )
#             # Join with contact_df to add contact columns
#             grantee_990pf_df = grantee_990pf_df.join(
#                 contact_df,
#                 ["object_id", "grantor_ein", "tax_year"],
#                 "left_outer"
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 grantee_990pf_df.return_type_cd,  # Disambiguate return_type_cd
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.RecipientUSAddress.AddressLine2Txt").isNotNull()) & (col("grantee.RecipientUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.RecipientUSAddress.AddressLine1Txt"), col("grantee.RecipientUSAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.RecipientUSAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.RecipientUSAddress.CityNm").alias("grantee_city"),
#                 col("grantee.RecipientUSAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.RecipientUSAddress.ZIPCd").alias("grantee_zip_code"),
#                 col("grantee.RecipientFoundationStatusTxt").alias("grantee_foundation_status"),
#                 col("grantee.GrantOrContributionPurposeTxt").alias("grantee_purpose"),
#                 col("grantee.Amt").alias("grantee_amount"),
#                 col("grantee.TotalGrantOrContriPdDurYrAmt").alias("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.TaxYr").alias("grant_paid_year"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_contributing_manager_name"),
#                 col("grantee_shareholder_manager_name"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 col("contact_person_name_for_grants"),
#                 col("contact_phone_num_for_grants"),
#                 col("contact_form_info_for_grants"),
#                 col("contact_submission_deadlines_for_grants"),
#                 col("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990)
#             grantee_990_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 explode(col("ReturnData.IRS990.IRS990ScheduleI.RecipientTable")).alias("grantee"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.TotalOtherOrgCnt").alias("grantee_other_org_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.GrantTypeTxt").alias("grantee_individual_grant_type"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.RecipientCnt").alias("grantee_individual_recipient_count"),
#                 col("ReturnData.IRS990.IRS990ScheduleI.GrantsOtherAsstToIndivInUSGrp.CashGrantAmt").alias("grantee_individual_cash_amount_other"),
#                 col("ReturnData.IRS990.GrantsToDomesticOrgsGrp.TotalAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990.TotalVolunteersCnt").alias("total_volunteers"),
#                 col("ReturnData.IRS990.ActivityOrMissionDesc").alias("mission_statement"),
#                 col("ReturnData.IRS990.FederatedCampaignsAmt").alias("federal_campaign_contributions"),
#                 col("ReturnData.IRS990.MembershipDuesAmt").alias("membership_dues"),
#                 col("ReturnData.IRS990.FundraisingAmt").alias("fundraising_contributions"),
#                 col("ReturnData.IRS990.RelatedOrganizationsAmt").alias("related_organization_support"),
#                 col("ReturnData.IRS990.GovernmentGrantsAmt").alias("government_grants"),
#                 col("ReturnData.IRS990.AllOtherContributionsAmt").alias("other_contributions")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 col("grantee.RecipientPersonNm").alias("grantee_name"),
#                 when((col("grantee.RecipientBusinessName.BusinessNameLine1Txt").isNotNull()) & 
#                      (col("grantee.RecipientBusinessName.BusinessNameLine1Txt") != ""),
#                      when((col("grantee.RecipientBusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                           (col("grantee.RecipientBusinessName.BusinessNameLine2Txt") != ""),
#                           concat_ws(" ",
#                               col("grantee.RecipientBusinessName.BusinessNameLine1Txt"),
#                               col("grantee.RecipientBusinessName.BusinessNameLine2Txt")
#                           ))
#                      .otherwise(col("grantee.RecipientBusinessName.BusinessNameLine1Txt")))
#                 .otherwise(col("grantee.RecipientPersonNm")).alias("grantee_business_name"),
#                 when((col("grantee.USAddress.AddressLine2Txt").isNotNull()) & (col("grantee.USAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ", col("grantee.USAddress.AddressLine1Txt"), col("grantee.USAddress.AddressLine2Txt")))
#                 .otherwise(col("grantee.USAddress.AddressLine1Txt")).alias("grantee_address"),
#                 col("grantee.USAddress.CityNm").alias("grantee_city"),
#                 col("grantee.USAddress.StateAbbreviationCd").alias("grantee_state_cd"),
#                 col("grantee.USAddress.ZIPCd").alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 col("grantee.PurposeOfGrantTxt").alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 col("grantee.OnlyContriToPreselectedInd").alias("grantee_preselected_indicator"),
#                 col("grantee.RecipientRelationshipTxt").alias("grantee_relationship"),
#                 col("grantee.RecipientEIN").alias("grantee_ein"),
#                 col("grantee.IRCSectionDesc").alias("grantee_irc_section_desc"),
#                 col("grantee.CashGrantAmt").alias("grantee_cash_amount"),
#                 col("grantee.Total501c3OrgCnt").alias("grantee_501c3_org_count"),
#                 col("grantee_other_org_count"),
#                 col("grantee_individual_grant_type"),
#                 col("grantee_individual_recipient_count"),
#                 col("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 col("total_volunteers"),
#                 col("mission_statement"),
#                 col("federal_campaign_contributions"),
#                 col("membership_dues"),
#                 col("fundraising_contributions"),
#                 col("related_organization_support"),
#                 col("government_grants"),
#                 col("other_contributions"),
#                 lit(None).cast(StringType()).alias("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Grantee DataFrame (990EZ)
#             grantee_990ez_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990EZ").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 col("ReturnHeader.ReturnTypeCd").alias("return_type_cd"),
#                 col("ReturnHeader.TaxPeriodBeginDt").alias("tax_period_begin_dt"),
#                 col("ReturnHeader.TaxPeriodEndDt").alias("tax_period_end_dt"),
#                 when((col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt").isNotNull()) &
#                      (col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt"),
#                          col("ReturnHeader.Filer.BusinessName.BusinessNameLine2Txt")
#                      )).otherwise(col("ReturnHeader.Filer.BusinessName.BusinessNameLine1Txt")).alias("grantor_business_name"),
#                 col("ReturnHeader.Filer.BusinessNameControlTxt").alias("grantor_business_name_control"),
#                 col("ReturnData.IRS990EZ.GrantsAndSimilarAmountsPaidAmt").alias("grantee_total_amount_in_year"),
#                 col("ReturnData.IRS990EZ.PrimaryExemptPurposeTxt").alias("primary_purpose_statement")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 col("grantor_business_name"),
#                 col("tax_period_begin_dt"),
#                 col("tax_period_end_dt"),
#                 col("grantor_business_name_control"),
#                 lit(None).cast(StringType()).alias("grantee_name"),
#                 lit(None).cast(StringType()).alias("grantee_business_name"),
#                 lit(None).cast(StringType()).alias("grantee_address"),
#                 lit(None).cast(StringType()).alias("grantee_city"),
#                 lit(None).cast(StringType()).alias("grantee_state_cd"),
#                 lit(None).cast(StringType()).alias("grantee_zip_code"),
#                 lit(None).cast(StringType()).alias("grantee_foundation_status"),
#                 lit(None).cast(StringType()).alias("grantee_purpose"),
#                 lit(None).cast(DoubleType()).alias("grantee_amount"),
#                 col("grantee_total_amount_in_year"),
#                 lit(None).cast(StringType()).alias("grantee_preselected_indicator"),
#                 lit(None).cast(StringType()).alias("grantee_relationship"),
#                 lit(None).cast(StringType()).alias("grantee_ein"),
#                 lit(None).cast(StringType()).alias("grantee_irc_section_desc"),
#                 lit(None).cast(DoubleType()).alias("grantee_cash_amount"),
#                 lit(None).cast(StringType()).alias("grantee_501c3_org_count"),
#                 lit(None).cast(IntegerType()).alias("grantee_other_org_count"),
#                 lit(None).cast(StringType()).alias("grantee_individual_grant_type"),
#                 lit(None).cast(StringType()).alias("grantee_individual_recipient_count"),
#                 lit(None).cast(DoubleType()).alias("grantee_individual_cash_amount_other"),
#                 lit(None).cast(StringType()).alias("grantee_contributing_manager_name"),
#                 lit(None).cast(StringType()).alias("grantee_shareholder_manager_name"),
#                 lit(None).cast(StringType()).alias("total_volunteers"),
#                 lit(None).cast(StringType()).alias("mission_statement"),
#                 lit(None).cast(DoubleType()).alias("federal_campaign_contributions"),
#                 lit(None).cast(DoubleType()).alias("membership_dues"),
#                 lit(None).cast(DoubleType()).alias("fundraising_contributions"),
#                 lit(None).cast(DoubleType()).alias("related_organization_support"),
#                 lit(None).cast(DoubleType()).alias("government_grants"),
#                 lit(None).cast(DoubleType()).alias("other_contributions"),
#                 col("primary_purpose_statement"),
#                 lit(None).cast(StringType()).alias("grant_paid_year"),
#                 lit(None).cast(StringType()).alias("contact_person_name_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_phone_num_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_form_info_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_submission_deadlines_for_grants"),
#                 lit(None).cast(StringType()).alias("contact_restrictions_for_grants")
#             )

#             # Union Grantee DataFrames
#             grantee_df = grantee_990pf_df.union(grantee_990_df).union(grantee_990ez_df)
#             logger.info("Extracted grantee information")

#             # Contributor DataFrame (990PF only)
#             contributor_990pf_df = df.filter(col("ReturnHeader.ReturnTypeCd") == "990PF").select(
#                 col("object_id"),
#                 col("ReturnHeader.Filer.EIN").alias("grantor_ein"),
#                 col("ReturnHeader.TaxYr").alias("tax_year"),
#                 lit("990PF").alias("return_type_cd"),
#                 explode(col("ReturnData.IRS990PF.IRS990ScheduleB.ContributorInformationGrp")).alias("contributor")
#             ).select(
#                 col("object_id"),
#                 col("grantor_ein"),
#                 col("tax_year"),
#                 col("return_type_cd"),
#                 when((col("contributor.ContributorPersonNm").isNotNull()) & (col("contributor.ContributorPersonNm") != ""),
#                      col("contributor.ContributorPersonNm"))
#                 .when((col("contributor.BusinessName.BusinessNameLine2Txt").isNotNull()) & 
#                       (col("contributor.BusinessName.BusinessNameLine2Txt") != ""),
#                       concat_ws(" ",
#                           col("contributor.BusinessName.BusinessNameLine1Txt"),
#                           col("contributor.BusinessName.BusinessNameLine2Txt")
#                       ))
#                 .otherwise(col("contributor.BusinessName.BusinessNameLine1Txt"))
#                 .alias("contributor_name"),
#                 when((col("contributor.ContributorUSAddress.AddressLine2Txt").isNotNull()) & 
#                      (col("contributor.ContributorUSAddress.AddressLine2Txt") != ""),
#                      concat_ws(" ",
#                          col("contributor.ContributorUSAddress.AddressLine1Txt"),
#                          col("contributor.ContributorUSAddress.AddressLine2Txt")
#                      )).otherwise(col("contributor.ContributorUSAddress.AddressLine1Txt")).alias("contributor_street_address"),
#                 col("contributor.ContributorUSAddress.CityNm").alias("contributor_city"),
#                 col("contributor.ContributorUSAddress.StateAbbreviationCd").alias("contributor_state"),
#                 col("contributor.ContributorUSAddress.ZIPCd").alias("contributor_zip_code"),
#                 col("contributor.TotalContributionsAmt").alias("total_contributed_by_contributor"),
#                 col("contributor.PersonContributionInd").alias("person_contribution_ind"),
#                 col("contributor.PayrollContributionInd").alias("payroll_contribution_ind"),
#                 col("contributor.NoncashContributionInd").alias("noncash_contribution_ind")
#             )
#             logger.info("Extracted contributor information for 990PF")

#             # Write each DataFrame to separate S3 paths
#             entity_dfs = [
#                 ("grantor", grantor_df),
#                 ("board_member", board_member_df),
#                 ("grantee", grantee_df),
#                 ("contact", contact_df),
#                 ("employee", employee_990pf_df),
#                 ("contributor", contributor_990pf_df)
#             ]

#             for entity_name, entity_df in entity_dfs:
#                 # Check if DataFrame is empty
#                 if entity_df.rdd.isEmpty():
#                     logger.warning(f"{entity_name} DataFrame is empty for {input_path}, skipping write")
#                     continue
                
#                 # Ensure required columns are present
#                 required_columns = ["object_id", "grantor_ein", "tax_year", "return_type_cd"]
#                 missing_columns = [col for col in required_columns if col not in entity_df.columns]
#                 if missing_columns:
#                     logger.warning(f"{entity_name} DataFrame missing required columns: {missing_columns}. Adding as null.")
#                     for col_name in missing_columns:
#                         entity_df = entity_df.withColumn(col_name, lit(None).cast(StringType()))
                
#                 # Define output path for this entity and year
#                 entity_output_path = f"{output_base_path}{entity_name}/year={year}/"
                
#                 # Write DataFrame to Parquet
#                 try:
#                     entity_df.repartition(100).write \
#                         .mode("append") \
#                         .parquet(entity_output_path)
#                     logger.info(f"Written {entity_name} Parquet files to {entity_output_path}")
#                 except Exception as e:
#                     logger.error(f"Error writing {entity_name} to {entity_output_path}: {str(e)}")
#                     continue

#         except Exception as e:
#             logger.error(f"Error processing {input_path}: {str(e)}")
#             continue
#         finally:
#             df.unpersist()
#             logger.info("DataFrame cache cleared")

# try:
#     job.commit()
#     logger.info("ETL job completed successfully!")
# except Exception as e:
#     logger.error(f"Job commit failed: {str(e)}")
#     raise