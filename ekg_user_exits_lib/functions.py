import logging

logger = logging.getLogger("o9_logger")

def verify_user_exit_install():
    # Call this function to verify installation of the library
    logger.info("EKG User Exit Library Installation Verified!!!")

    
def custom_staging_query(spark, source_table_name, target_table_name):
    # user defined logic here return None for default
    return None


def custom_join_query(spark, source_table_list, target_table_name):
    if target_table_name == 'LSU_LocationMaster':
        odm_locationmaster_df = spark.sql("SELECT * FROM odm_locationmaster")
        lsu_locationmaster = odm_locationmaster_df.select(col("`All Location`").alias("[Location].[All Location]"),col("Location").alias("[Location].[Location]"),col("ReportingLocation").alias("[Location].[Reporting Location]"),col("PlanningLocation").alias("[Location].[Planning Location]"),col("Location$DisplayName").alias("[Location].[Location$DisplayName]"),col("`Plant Description`").alias("[Location].[Plant Description]"),col("LocationRegion").alias("[Location].[Location Region]"),col("LocationCountry").alias("[Location].[Location Country]"),lit("LocationType").alias("[Location].[Location Type]"),col("`Location City`").alias("[Location].[Location City]"))
        return lsu_locationmaster
    # user defined logic here return None for default
    return None


def prevalidate_dataframe(spark, target_table_dataframe, target_table_name, fields):
    # user defined logic here return None for default
    return None


def postvalidate_dataframe(spark, target_table_dataframe, target_table_name, fields):
    # user defined logic here return None for default
    return None


def execute_user_exit_code(operation, **kwargs):
    operations = {
        'stagingQuery': custom_staging_query,
        'joinQuery': custom_join_query,
        'preValidate': prevalidate_dataframe,
        'postValidate': postvalidate_dataframe
    }

    # Get the function based on the operation
    func = operations.get(operation)

    if func:
        try:
            return func(**kwargs)
        except Exception as e:
            logger.error(
                f"<EKG User Exits> Error in User Defined Function: {e}")
            return None

    else:
        logger.error(
            f"<EKG User Exits> Invalid operation recieved: {operation}")
        return None
