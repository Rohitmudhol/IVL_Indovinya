# User Exits
This README provides an explanation of the functions defined for User Exit processes. Each function is designed to allow users to define their logic for specific stages of the ETL workflow.

# Getting Started
To make use of the User Exits functionality, follow these steps:
1.	**Clone this Repository**: Begin by cloning the default repository that contains the template functions you will need to customize.

2.	**Add Custom Code**: Navigate to the functions.py file in the cloned repository and locate the functions provided for customization. Add your custom code to the functions to achieve the desired mutations, Look [Function Definitions](##-Function-Definitions) and [Usage](##-Usage).

3.	**Library Installation**: Install the remote repository in your deployment tenant

## Function Definitions

### 1. `custom_staging_query`

This function allows the user to define a custom SQL query to select source tables. If no custom logic is provided, the function returns `None`.

#### Parameters:
- **spark**: The Spark context.
- **source_table_name**: The name of the source table.
- **target_table_name**: The name of the target table.

#### Returns:
- Custom query (string) logic or `None` as default.

### 2. `custom_join_query`

This function allows the user to define a SQL query for joining source tables that will be written into the target table. If no custom logic is provided, the function returns `None`.

#### Parameters:
- **spark**: The Spark context.
- **source_table_list**: A list of names of all source tables associated with the target table.
- **target_table_name**: The name of the target table.

#### Returns:
- Custom query (string) logic or `None` as default.

### 3. `prevalidate_dataframe`

This function allows the user to define custom logic for the target table's DataFrame before the all validation checks. If no custom logic is provided, the function returns `None`.

#### Parameters:
- **spark**: The Spark context.
- **target_table_dataframe**: The DataFrame of the target table.
- **target_table_name**: The name of the target table.
- **fields**: A list of dictionaries containing information about the fields of the target table. Each dictionary includes the field name, position, and data type.

#### Returns:
- pyspark dataframe or `None` as default.

### 4. `postvalidate_dataframe`

This function allows the user to define custom post-validation logic for the target table's DataFrame after the validation checks are done. If no custom logic is provided, the function returns `None`.

#### Parameters:
- **spark**: The Spark context.
- **target_table_dataframe**: The DataFrame of the target table.
- **target_table_name**: The name of the target table.
- **fields**: A list of dictionaries containing information about the fields of the target table. Each dictionary includes the field name, position, and data type.

#### Returns:
- pyspark dataframe or `None` as default.

## Usage

To use these functions, define your custom logic within the respective function bodies. If no custom logic is needed, return `None`, indicating that the default process should be used, refer to the below example.

```python

def custom_staging_query(spark, source_table_name, target_table_name):
    # user defined logic here return None for default
    if source_table_name == "In_SalesOrderCommit" and target_table_name == "ODM_SalesOrderCommit":
        query = "SELECT CAST(SalesOrderHeaderID AS String) AS `SalesOrderHeaderID`, CAST(SalesOrderLineID AS String) AS `SalesOrderLineID`, CAST(Item AS String) AS `Item`, CAST(Location AS String) AS `Location`, CAST(Account AS String) AS `Account`, CAST(Region AS String) AS `Region`, CAST(Channel AS String) AS `Channel`, CAST(SOCommitActualDeliveryDateInput AS String) AS `SOCommitActualDeliveryDateInput`, CAST(SOCommitDeliveredQuantityInput AS Double) AS `SOCommitDeliveredQuantityInput`, CAST(SOCommitQuantityInput AS Double) AS `SOCommitQuantityInput`, CAST(SOCommitStatusInput AS String) AS `SOCommitStatusInput`, CAST(SOCommitTypeInput AS String) AS `SOCommitTypeInput` FROM In_SalesOrderCommit WHERE is_valid IN (1, -1) AND CAST(SOCommitDeliveredQuantityInput AS Double) > 100;"
        return query

    return None


def custom_join_query(spark, source_table_list, target_table_name):
    # user defined logic here return None for default
    if target_table_name == "ODM_SalesOrderCommit":
        query = "SELECT CAST(SalesOrderHeaderID AS String) AS `SalesOrderHeaderID`, CAST(SalesOrderLineID AS String) AS `SalesOrderLineID`, CAST(Item AS String) AS `Item`, CAST(Location AS String) AS `Location`, CAST(Account AS String) AS `Account`, CAST(Region AS String) AS `Region`, CAST(Channel AS String) AS `Channel`, CAST(SOCommitActualDeliveryDateInput AS String) AS `SOCommitActualDeliveryDateInput`, CAST(SOCommitDeliveredQuantityInput AS Double) AS `SOCommitDeliveredQuantityInput`, CAST(SOCommitQuantityInput AS Double) AS `SOCommitQuantityInput`, CAST(SOCommitStatusInput AS String) AS `SOCommitStatusInput`, CAST(SOCommitTypeInput AS String) AS `SOCommitTypeInput` FROM In_SalesOrderCommit_temp WHERE SOCommitDeliveredQuantityInput < 450;"
        return query

    return None


def prevalidate_dataframe(spark, target_table_dataframe, target_table_name, fields):
    # user defined logic here return None for default
    if target_table_name == "ODM_SalesOrderCommit":
        df = target_table_dataframe.withColumn("SOCommitQuantityInput", col("SOCommitQuantityInput") + 10)
        return df

    return None


def postvalidate_dataframe(spark, target_table_dataframe, target_table_name, fields):
    # user defined logic here return None for default
    if target_table_name == "In_SalesOrderCommit":
        df = target_table_dataframe.withColumn("SOCommitDeliveredQuantityInput", round(col("SOCommitDeliveredQuantityInput").cast("double"), 0).cast("string"))
        return df

    return None
```

These functions provide flexibility for customizing various stages of your ETL process according to your specific requirements.