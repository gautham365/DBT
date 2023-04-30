import findspark
findspark.init()
import pyspark
import socket
import json
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName="newsAPI")
ssc = StreamingContext(sc, 5)
spark = SparkSession.builder.appName("newsAPI").getOrCreate()

lines = ssc.socketTextStream("localhost", 9999)

kafka_host = "localhost"
kafka_port = 5558

def sendtheresult(rdd):
    if not rdd.isEmpty():
        res=rdd.collect()
        # print("Sending data to Kafka:",res[0])
        send_kafka(str(res[0]))

def send_kafka(res):
    # Create a socket connection to the Kafka producer
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as kafka_socket:
        kafka_socket.connect((kafka_host, kafka_port))
        # Send data to the Kafka producer via socket
        kafka_socket.sendall(res.encode())


# Print the first ten elements of each RDD generated in this DStream to the console
lines.foreachRDD(lambda rdd: print_rdd(rdd))



# Define a function to print the data in each RDD
def print_rdd(rdd):
    # for record in rdd.collect():
        # print("Data",record)
    sendtheresult(rdd)
        # data = json.loads(record)
        # print("Title: ", data['title'])
        # print("URL: ", data['url'])
        # print("Content: ", data['content'])

# Use foreachRDD() to apply the print_rdd() function to each RDD in the DStream
# words.foreachRDD(lambda rdd: print_rdd(rdd))

# Start the streaming context
ssc.start()
ssc.awaitTermination()
