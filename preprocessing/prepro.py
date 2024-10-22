import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, collect_list, countDistinct
from pyspark.sql.types import StringType
from datetime import datetime
from graphframes import GraphFrame

# Initialize Spark session 
spark = SparkSession.builder \
    .appName("PhoneCallsCommunityDetection") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Set a checkpoint directory for Spark
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

file_path = 'adjusted_phone_calls.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert YYMMDDHHMM to a proper datetime object
def convert_to_datetime(yyMMddHHMM):
    return datetime.strptime(str(yyMMddHHMM), '%y%m%d%H%M')

# Define UDF for calculating duration in DDHHMM format
def convert_duration_to_DDHHMM(start_time, end_time):
    start_dt = convert_to_datetime(start_time)
    end_dt = convert_to_datetime(end_time)
    duration = end_dt - start_dt
    
    days = duration.days
    hours, remainder = divmod(duration.seconds, 3600)
    minutes = remainder // 60
    return f'{days:02d}{hours:02d}{minutes:02d}'

# Register the UDF in Spark
convert_duration_udf = udf(convert_duration_to_DDHHMM, StringType())

# Add a column for duration in DDHHMM format
df = df.withColumn('duration_DDHHMM', convert_duration_udf(col('Start_Time'), col('End_Time')))

# Create Graph using GraphFrames for community detection
vertices = df.selectExpr("Client1 as id").union(df.selectExpr("Client2 as id")).distinct()
edges = df.selectExpr("Client1 as src", "Client2 as dst")

# Cache vertices and edges
vertices.cache()
edges.cache()

# Create a GraphFrame
g = GraphFrame(vertices, edges)

# Find connected components (communities) using GraphFrames
result = g.connectedComponents()

# Join the result (community IDs) with the original dataframe
df_with_communities = df.join(result, df['Client1'] == result['id'], 'inner').withColumnRenamed('component', 'community_id')

# Calculate the number of unique clients (community size) per community
community_sizes = df_with_communities.select("community_id", "Client1").union(df_with_communities.select("community_id", "Client2")) \
    .distinct() \
    .groupBy("community_id").agg(countDistinct("Client1").alias("community_size"))

# Merge the community sizes into the main DataFrame
df_final = df_with_communities.join(community_sizes, 'community_id')

# Get list of tuples for each community member by considering both Client1 and Client2
community_members = df_final.select("community_id", "Client1").union(df_final.select("community_id", "Client2")) \
    .distinct() \
    .groupBy("community_id").agg(collect_list("Client1").alias("members"))

# Show the final DataFrame with community IDs, duration, and community sizes
df_final.select('Client1', 'Client2', 'duration_DDHHMM', 'community_id', 'community_size').show()

# Show the list of community members as tuples
community_members.show()

