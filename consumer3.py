from kafka import KafkaConsumer

# List of Kafka topics to subscribe to
topics = 'election'

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']

# Kafka consumer group ID
group_id = 'Gujarati-Dhokla'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    topics,
    bootstrap_servers=bootstrap_servers
)

# Continuously poll for new messages from K=
for message in consumer:
	# Process the received message
	msg = message.value.decode('utf-8')
	print("Received message:", msg)

# Using Aggregate queries
'''
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema of the Kafka message
schema = StructType([
    StructField("title", StringType()),
    StructField("url", StringType()),
    StructField("content", StringType())
])

# Read data from Kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "bitcoin") \
  .option("startingOffsets", "earliest") \
  .load()

# Parse the Kafka message and extract the event timestamp
parsed_df = df \
  .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
  .select("parsed_value.timestamp", "parsed_value.event")

# Aggregate events by minute
event_count_df = parsed_df \
  .agg(count("title").alias("news_count"))

# Write the result to console
console_query = event_count_df \
  .writeStream \
  .format("console") \
  .outputMode("complete") \
  .start()

console_query.awaitTermination()'''

