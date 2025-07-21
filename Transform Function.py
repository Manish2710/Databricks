# Databricks notebook source
# MAGIC %md
# MAGIC **Sample DataFrame**

# COMMAND ----------

from pyspark.sql.functions import col

data = [
  ("Product1", 100, "Category1"),
  ("Product2", 200, "Category2"),
  ("Product3", 150, "Category1")
]

columns = ["Production", "Price", "Category"]

df = spark.createDataFrame(data, columns)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform Function**

# COMMAND ----------

def transform_function(df):
  return df.withColumn("DiscountedPrice", col("Price") * 0.9)

transformed_df = df.transform(transform_function)

transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform with Parameters**

# COMMAND ----------

from pyspark.sql.functions import col

data = [
  ("Product1", 100, "Category1"),
  ("Product2", 200, "Category2"),
  ("Product3", 150, "Category1")
]

columns = ["Production", "Price", "Category"]

df = spark.createDataFrame(data, columns)

df.display()

def transform_function(df, discount_percentage):
  return df.withColumn("DiscountedPrice", col("Price") * (1 - discount_percentage / 100))

percentage = 10

#transformed_df = df.transform(transform_function, percentage)
transformed_df = df.transform(transform_function, discount_percentage = percentage)
#transformed_df = df.transform(lambda df: transform_function(df, percentage))

transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparison - Without Transform**

# COMMAND ----------

from pyspark.sql.functions import col, to_date, year, month, length, abs, when, last_day, date_format, expr

# Sample data
data = [
    ('TXN001', '2023-02-18', 250.75, 'Electronics', 'Bought a new phone'),
    ('TXN002', '2024-02-25', -50.50, 'Groceries', 'Refunded groceries'),
    ('TXN003', '2019-03-01', 125.00, 'Clothing', 'Purchased new jacket'),
    ('TXN004', '2024-11-28', -10.00, 'Books', 'Refunded book purchase')
]

# Schema definition
schema = 'TransactionID string, TransactionDate string, Amount float, Category string, Description string'

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Apply transformations

df = (
    df
    .withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd"))  # 1. Convert to date
    .withColumn("Year", year(col("TransactionDate")))                              # 2. Extract year
    .withColumn("Month", month(col("TransactionDate")))                            # 3. Extract month
    .withColumn("Description_Length", length(col("Description")))                  # 4. Length of Description
    .withColumn("Amount_Abs", abs(col("Amount")))                                   # 5. Absolute value of Amount
    .withColumn("Is_Refund", when(col("Amount") < 0, True).otherwise(False))        # 6. Indicate if refund
    .withColumn("Last_Day_Of_Month", last_day(col("TransactionDate")))             # 7. Last day of month
    .withColumn("Formatted_Date", date_format(col("TransactionDate"), "yyyy-MM"))   # 8. Formatted date
    .withColumn("Transaction_Size", when(col("Amount_Abs") > 100, "Large").otherwise("Small"))  # 9. Categorize
    .withColumn(
        "Dynamic_Calculation",
        expr("""
        CASE
            WHEN Category = 'Electronics' THEN Amount * 1.10
            WHEN Category = 'Groceries' THEN Amount * 0.90
            ELSE Amount
        END
        """)                                                                         # 10. Dynamic calculation based on Category
    )
)



# COMMAND ----------

from pyspark.sql.functions import col, to_date, year, month, length, abs, when, last_day, date_format, expr

# Sample data
data = [
    ('TXN001', '2023-02-18', 250.75, 'Electronics', 'Bought a new phone'),
    ('TXN002', '2024-02-25', -50.50, 'Groceries', 'Refunded groceries'),
    ('TXN003', '2019-03-01', 125.00, 'Clothing', 'Purchased new jacket'),
    ('TXN004', '2024-11-28', -10.00, 'Books', 'Refunded book purchase')
]

# Schema
schema = 'TransactionID string, TransactionDate string, Amount float, Category string, Description string'

# Create DataFrame
df = spark.createDataFrame(data, schema)

# ==============================
# Transform functions
# ==============================

def convert_to_date(df):
    return df.withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyy-MM-dd"))

def extract_year(df):
    return df.withColumn("Year", year(col("TransactionDate")))

def extract_month(df):
    return df.withColumn("Month", month(col("TransactionDate")))

def description_length(df):
    return df.withColumn("Description_Length", length(col("Description")))

def absolute_amount(df):
    return df.withColumn("Amount_Abs", abs(col("Amount")))

def indicate_refund(df):
    return df.withColumn("Is_Refund", when(col("Amount") < 0, True).otherwise(False))

def last_day_of_month(df):
    return df.withColumn("Last_Day_Of_Month", last_day(col("TransactionDate")))

def formatted_date(df):
    return df.withColumn("Formatted_Date", date_format(col("TransactionDate"), "yyyy-MM"))

def categorize_transaction(df):
    return df.withColumn("Transaction_Size", when(col("Amount_Abs") > 100, "Large").otherwise("Small"))

def dynamic_calculation(df):
    return df.withColumn(
        "Dynamic_Calculation",
        expr("""
            CASE
                WHEN Category = 'Electronics' THEN Amount * 1.10
                WHEN Category = 'Groceries' THEN Amount * 0.90
                ELSE Amount
            END
        """)
    )

# ==============================
# Apply transformations
# ==============================
df = (
    df.transform(convert_to_date)
      .transform(extract_year)
      .transform(extract_month)
      .transform(description_length)
      .transform(absolute_amount)
      .transform(indicate_refund)
      .transform(last_day_of_month)
      .transform(formatted_date)
      .transform(categorize_transaction)
      .transform(dynamic_calculation)
)

# Show results
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Cleansing Usecase**

# COMMAND ----------

from pyspark.sql.functions import col, lit, avg, sum as _sum

# Sample DataFrame
data = [
    ("Alice", 34, "2023-06-01", 3000.0),
    ("Bob", 45, "2023-06-02", None),
    ("Cathy", None, "2023-06-03", 2500.0),
    ("Alice", 34, "2023-06-01", 3000.0),
    (None, 45, None, 4000.0)
]
df = spark.createDataFrame(data, ["Name", "Age", "Date", "Salary"])

# Fill values
name_fill = "Unknown"
age_fill = 0
date_fill = "1900-01-01"
salary_fill = 0.0
age_threshold = 30
salary_threshold = 3000.0
bonus_percentage = 0.10

# === Transformation functions ===
def handle_nulls(df, name_fill, age_fill, date_fill, salary_fill):
    return df.fillna({'Name': name_fill, 'Age': age_fill, 'Date': date_fill, 'Salary': salary_fill})

def remove_duplicates(df):
    return df.dropDuplicates()

def standardize_data_types(df):
    return df.withColumn("Age", col("Age").cast("int")) \
             .withColumn("Date", col("Date").cast("date")) \
             .withColumn("Salary", col("Salary").cast("double"))

def filter_rows(df, age_threshold, salary_threshold):
    return df.filter((col("Age") >= age_threshold) & (col("Salary") >= salary_threshold))

def add_bonus_column(df, bonus_percentage):
    return df.withColumn("Bonus", col("Salary") * lit(bonus_percentage))

def group_and_aggregate(df):
    return df.groupBy("Name").agg(
        avg("Age").alias("Average_Age"),
        _sum("Salary").alias("Total_Salary"),
        _sum("Bonus").alias("Total_Bonus")
    )

# === Apply transformations ===
df_transformed = (
    df.transform(lambda df: handle_nulls(df, name_fill, age_fill, date_fill, salary_fill))
      .transform(remove_duplicates)
      .transform(standardize_data_types)
      .transform(lambda df: filter_rows(df, age_threshold, salary_threshold))
      .transform(lambda df: add_bonus_column(df, bonus_percentage))
      .transform(group_and_aggregate)  # FINAL step
)

# === Show result ===
df_transformed.display()

# COMMAND ----------

