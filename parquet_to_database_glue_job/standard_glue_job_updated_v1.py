import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, lit, expr, first, lpad
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import boto3
from botocore.exceptions import ClientError

# Below is fr Settingup logging
logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info('Logger enabled')

# Initialize boto3 clients
secrets_client = boto3.client('secretsmanager')

# Retrieve PostgreSQL credentials from Secrets Manager
try:
    secret_name = "lumino-dev-secrets-manager"
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    credentials = json.loads(secret)
    username = credentials['username']
    password = credentials['password']
except ClientError as e:
    logger.error(f"Error retrieving secret: {e}")
    raise
except json.JSONDecodeError as e:
    logger.error(f"Invalid JSON in secret: {e}")
    raise

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args["JOB_NAME"], args)

# Defining JDBC URl
jdbc_url = "jdbc:postgresql://lumino-dev-db.c5aiiisqufhi.us-east-1.rds.amazonaws.com/luminodb"

try:
    # Step 1: Load data from PostgreSQL
    logger.info("Loaing data from S3 parquet files...")
    
    source_df = glueContext.create_dynamic_frame.from_catalog(
        database="standard_database_update1",
        table_name="std_parquet",
        transformation_ctx="source_df"
    ).toDF()
    
    # source_df = source_dynamic_frame.toDF()
    logger.info("Source schema: " + source_df.schema.simpleString())
    logger.info(f"Number of records after Step 1 (Load data): {source_df.count()}")

    # Logic to drop rows where ein is null or 0 and count dropped rows (before casting/padding)
    logger.info("Dropping rows where ein is null or 0 and counting dropped rows...")
    total_rows_before = source_df.count()
    filtered_df = source_df.filter(~(col("ein").isNull() | (col("ein") == 0)))
    total_rows_after = filtered_df.count()
    dropped_rows = total_rows_before - total_rows_after
    logger.info(f"Number of rows dropped due to null or 0 ein: {dropped_rows}")
    source_df = filtered_df

    # Step 2: Cast ein to string and pad to 10 characters with leading zeros
    logger.info("Casting ein from bigint to string and padding to 09 characters...")
    source_df = source_df.withColumn("ein", lpad(col("ein").cast("string"), 9, "0"))
   
    logger.info(f"Number of records after Step 2 (Cast and pad ein): {source_df.count()}")

    # Step 3: Impute null subsection_code
    logger.info("Imputing null subsection_code from other tax_year records before deduplication...")
    imputed_df = source_df.withColumn(
        "subsection_code",
        when(col("subsection_code").isNull(),
             first("subsection_code", ignorenulls=True).over(
                 Window.partitionBy("ein").orderBy(col("tax_year").desc())
             )
        ).otherwise(col("subsection_code"))
    )
    
    logger.info(f"Number of records after Step 3 (Impute subsection_code): {imputed_df.count()}")

    # Step 4: Remove duplicates based on ein and tax_year, keeping latest tax_period
    logger.info("Removing duplicates based on ein and tax_year, keeping latest tax_period...")
   
    sorted_df = imputed_df.orderBy(col("ein").asc(), col("tax_year").desc(), col("tax_period").desc())
    deduplicated_df = sorted_df.dropDuplicates(["ein", "tax_year"])
    deduplicated_df = deduplicated_df.repartition(20)
    deduplicated_df.cache()
    
    logger.info(f"Number of records after Step 4 (Remove duplicates): {deduplicated_df.count()}")

    # Step 5: Clean and impute values with corrected conditions
    logger.info("Cleaning and imputing values in specified columns...")
    cleaned_df = deduplicated_df\
        .withColumn("give_grants",
                    when((col("give_grants").isNull()) | (col("give_grants") == ""),
                         when(col("grants_paid") > 0, lit("Y")).otherwise(lit("N"))
                    ).otherwise(col("give_grants")))\
        .withColumn("total_contributions",
                    when((col("total_contributions") < 0) | (col("total_contributions").isNull()) | (col("total_contributions") == 1), lit(0))
                    .otherwise(col("total_contributions")))\
        .withColumn("total_revenue",
                    when(col("total_revenue").isNull(),
                         F.avg(F.when(col("total_revenue").isNotNull(), col("total_revenue"))).over(Window.partitionBy("subsection_code")))
                    .otherwise(col("total_revenue")))\
        .withColumn("total_expenses",
                    when(col("total_expenses").isNull(),
                         F.avg(F.when(col("total_expenses").isNotNull(), col("total_expenses"))).over(Window.partitionBy("subsection_code")))
                    .otherwise(col("total_expenses")))\
        .withColumn("total_assets",
                    when(col("total_assets").isNull(),
                         F.avg(F.when(col("total_assets").isNotNull(), col("total_assets"))).over(Window.partitionBy("subsection_code")))
                    .otherwise(col("total_assets")))\
        .withColumn("total_liabilities",
                    when(col("total_liabilities").isNull(),
                         F.avg(F.when(col("total_liabilities").isNotNull(), col("total_liabilities"))).over(Window.partitionBy("subsection_code")))
                    .otherwise(col("total_liabilities")))\
        .withColumn("total_networth",
                    when(col("total_networth").isNull(),
                         when(F.try_subtract(col("total_assets"), col("total_liabilities")).isNotNull(),
                              F.try_subtract(col("total_assets"), col("total_liabilities")))
                         .otherwise(F.avg(col("total_networth")).over(Window.partitionBy("subsection_code")))
                    ).otherwise(col("total_networth")))\
        .withColumn("organizations_supported",
                    when((col("organizations_supported").isNull()) & ((col("total_support").isNull()) | (col("total_support") == 0)), lit(0))
                    .when((col("organizations_supported").isNull()) & (col("total_support") > 0),
                          (col("total_support") / F.avg(col("total_support")).over(Window.partitionBy("subsection_code"))) * col("total_support"))
                    .otherwise(col("organizations_supported")))\
        .withColumn("total_support",
                    when((col("total_support").isNull()) & ((col("organizations_supported").isNull()) | (col("organizations_supported") == 0)), lit(0))
                    .when((col("total_support").isNull()) & (col("organizations_supported") > 0),
                          F.avg(col("total_support")).over(Window.partitionBy("subsection_code")))
                    .otherwise(col("total_support")))\
        .withColumn("grants_paid",
                    when(col("grants_paid") < 0, F.abs(col("grants_paid")))
                    .when((col("grants_paid").isNull()) | (col("grants_paid") == 0),
                          when(col("give_grants") == "N", lit(0))
                          .otherwise(F.avg(F.when((col("grants_paid") > 0) & (col("give_grants") == "Y"), col("grants_paid")))
                                   .over(Window.partitionBy("subsection_code")))
                    ).otherwise(col("grants_paid")))
    
    logger.info(f"Number of records after Step 5 (Clean and impute values): {cleaned_df.count()}")

    # Step 6: Select and cast columns
    logger.info("Selecting and casting columns to match PostgreSQL schema...")
    
    final_df_calc = cleaned_df.select(
        col("ein").cast("string").alias("ein"),
        col("tax_period").cast("integer").alias("tax_period"),
        col("tax_year").cast("integer").alias("tax_year"),
        col("give_grants").cast("string").alias("give_grants"),
        col("subsection_code").cast("integer").alias("subsection_code"),
        col("total_contributions").cast("decimal(20,4)").alias("total_contributions"),
        col("total_revenue").cast("decimal(20,4)").alias("total_revenue"),
        col("total_expenses").cast("decimal(20,4)").alias("total_expenses"),
        col("total_assets").cast("decimal(20,4)").alias("total_assets"),
        col("total_liabilities").cast("decimal(20,4)").alias("total_liabilities"),
        col("total_networth").cast("decimal(20,4)").alias("total_networth"),
        col("organizations_supported").cast("integer").alias("organizations_supported"),
        col("total_support").cast("decimal(20,4)").alias("total_support"),
        col("grants_paid").cast("decimal(20,4)").alias("grants_paid"),
        col("program_service_revenue").cast("decimal(20,4)").alias("program_service_revenue"),
        col("fundraising_revenue").cast("decimal(20,4)").alias("fundraising_revenue"),
        col("less_direct_fundraising").cast("decimal(20,4)").alias("less_direct_fundraising"),
        col("net_fundraising_income").cast("decimal(20,4)").alias("net_fundraising_income"),
        col("grants_to_govt").cast("decimal(20,4)").alias("grants_to_govt"),
        col("grants_to_individuals").cast("decimal(20,4)").alias("grants_to_individuals"),
        col("foreign_grants").cast("decimal(20,4)").alias("foreign_grants"),
        col("cease_operations").cast("string").alias("cease_operations"),
        col("grant_to_govt_indicator").cast("string").alias("grant_to_govt_indicator"),
        col("grant_to_individuals_indicator").cast("string").alias("grant_to_individuals_indicator"),
        col("file_type").cast("string").alias("file_type")
    )
    
    logger.info(f"Number of records after Step 6 for final_df_calc: {final_df_calc.count()}")


    # Step 7: Write to PostgreSQL with transaction support (Filter out existing rows)
    # Load existing (ein, tax_year) pairs from public.financials_calc
    logger.info("Loading existing (ein, tax_year) pairs from public.financials...")
    existing_calc_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable", "public.financials")\
        .option("user", username)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .load()\
        .select("ein", "tax_year")

    # Filter out rows in final_df_calc that already exist in public.financials_calc
    logger.info("Filtering out existing rows from final_df_calc...")
    new_calc_df = final_df_calc.join(
        existing_calc_df,
        (final_df_calc.ein == existing_calc_df.ein) & (final_df_calc.tax_year == existing_calc_df.tax_year),
        "left_anti"
    )
    
    #getting the ein from the organizations table
    # Load existing (ein, tax_year)  from public.organizations
    logger.info("Loading existing (ein, tax_year) pairs from public.organizations...")
    existing_ein = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable", "public.organizations")\
        .option("user", username)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .load()\
        .select("ein", "name")
    
    #Identify eins in new_calc_df that are not in existing_ein
    logger.info("Identifying ein values in new_calc_df that are not in public.organizations...")
    missing_eins = new_calc_df.join(
        existing_ein,
        new_calc_df.ein == existing_ein.ein,
        "left_anti"
    ).select(new_calc_df.ein).distinct()
    
    # Add name = 'UNKNOWN' to missing eins
    missing_eins_with_name = missing_eins.withColumn("name", lit("UNKNOWN"))
    
    # Log the number of missing eins
    missing_count = missing_eins_with_name.count()
    logger.info(f"Number of missing ein values to insert into public.organizations: {missing_count}")
    
    # If there are missing eins, write them to public.organizations
    if missing_count > 0:
        logger.info("Inserting missing ein values into public.organizations...")
        missing_eins_dynamic_frame = DynamicFrame.fromDF(missing_eins_with_name, glueContext, "missing_eins_dynamic_frame")
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=missing_eins_dynamic_frame,
            catalog_connection="xml_db_connection",
            connection_options={
                "dbtable": "public.organizations",
                "database": "luminodb",
                "url": jdbc_url,
                "user": username,
                "password": password
            },
            transformation_ctx="missing_eins_target"
        )
        logger.info("Successfully inserted missing ein values into public.organizations.")
    else:
        logger.info("No missing ein values to insert into public.organizations.")
        
    # Filter rows to only include supported tax_year values (2019–2024)
    logger.info("Filtering rows to only include tax_year between 2019 and 2024...")
    supported_tax_years = [2019, 2020, 2021, 2022, 2023, 2024, 2025]
    new_calc_df = new_calc_df.filter(col("tax_year").isin(supported_tax_years))
    logger.info(f"Number of records after filtering tax_year: {new_calc_df.count()}")
    
    final_dynamic_frame_calc = DynamicFrame.fromDF(new_calc_df, glueContext, "final_dynamic_frame_calc")
    logger.info(f"Number of new records to write to public.financials: {final_dynamic_frame_calc.count()}")

    # Step 7: Convert back to DynamicFrame and write output
    # logger.info("Converting to DynamicFrame and writing output...")
    # final_dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "final_dynamic_frame")

    glueContext.write_dynamic_frame.from_catalog(
        frame=final_dynamic_frame_calc,
        database="standard_database_update1",
        table_name="std_luminodb_public_financials",
        transformation_ctx="target_df",
        additional_options={"writeDisposition": "WRITE_APPEND"}  # Use WRITE_TRUNCATE to overwrite
    )

    logger.info("Job for clean data is completed successfully.")
    
except Exception as e:
    logger.error("Key error occurred during the job: %s", str(e))
    job.commit()
    raise
