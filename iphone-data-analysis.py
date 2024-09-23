from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

#creating spark session or entry point  
spark = SparkSession.builder \
    .appName("iphone-data-analysis-df") \
    .getOrCreate()


# Reading the data set from S3
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("s3://your-bucket-name/path-to-your-data/apple_products.csv")



#checking out all the columns and its data types 
df.printSchema()


#Listing out maximum and minimum MRP prices
df.select(max(col("Mrp")).alias("Max Mrp"), min(col("Mrp")).alias("Min Mrp")).show()

#Using where condition to get a list of records which has the specific mrp value that is 149900
df.where("Mrp = 149900").show()

#Creating a view to perform SQL Queries directly on the dataframe
df.createOrReplaceTempView("apple_table")


#listing of product names whose sum of mrp is greater than 100000 using sql query on the created View
spark.sql("""
select `Product Name`, SUM(Mrp) as sum_mrp
from apple_table
group by 1
""").where("sum_mrp > 100000").show()


# Discounting 10% of the MRP and calculating the latest price
df = df.withColumn("disc_price", col("Mrp") * 0.1)\
       .withColumn("new_price", col("Mrp") - col("disc_price"))

# Show the new columns
df.select("Product Name", "Mrp", "disc_price", "new_price").show(5)


# Extracting the model name from the Product name
df = df.withColumn("Model Name", substring(col("Product Name"), 7, 8))


# Show the Model Name column
df.select("Product Name", "Model Name").show(5)


#Listing out the Star Rating and its count for each rating
df.groupBy(col("Star Rating")).count().orderBy(desc("Star Rating")).show()


#filter products with discount greater than 10%
df.filter(col("Discount Percentage") > 10).show(10)


#Calcuate Average Sale price by Ram
df.groupBy("Ram").agg(avg("Sale Price").alias("Avg Sale Price")).show()


#Top 5 products with Highest number of reviews 
df.orderBy(col("Number Of Reviews").desc()).limit(5).select("Product Name", "Number Of Reviews").show(5, False)


#count the number of products for each star rating 
df.groupBy(col("Star Rating")).count().orderBy("count", ascending=False).show()


#filter products with Star Rating less than or equal to 4.5 and having Number of Reviews more than 1000
df.filter((col("Star Rating") <= 4.5) & (col("Number of Reviews") > 1000)).show()


#calculating avg no of ratings by ram 
df.groupBy(col("Ram")).agg(avg(col('Number Of Ratings')).alias("Average No of Ratings")).show()


#adding a new column indicating high discount 
df = df.withColumn("High Discount", col("Discount Percentage") > 20)
df.select("Product Name", "Discount Percentage", "High Discount").show(5)


#products with lowest sale price 
df.orderBy(col("Sale Price").asc()).limit(5).select("Product Name", "Sale Price").show()



def categorize_rating(rating):
    if rating >= 4.7:
        return "Excellent"
    elif rating == 4.6:
        return "Good"
    elif rating <= 4.5:
        return "Average"
    else:
        return "Poor"
    


#register the UDF
categorize_rating_udf = udf(categorize_rating, StringType())


#Apply UDF to create a new column based on rating
df = df.withColumn("Rating Category", categorize_rating_udf(col("Star Rating")))
df.select("Star Rating", "Rating Category").show()


# Creating another dataframe for joining
df2 = df.select("Product Name", "Sale Price", "Mrp", "Number Of Reviews", "Ram")\
    .withColumnRenamed("Sale Price", "Price")

# Joining both dataframes
joined_df = df.join(df2, on="Product Name", how="inner")

# Saving the joined dataframe to S3 in parquet format
joined_df.write.format("parquet")\
    .mode("overwrite")\
    .save("s3://your-bucket-name/path-to-save-data/joined_apple_products.parquet")


