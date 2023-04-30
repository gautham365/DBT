from kafka import KafkaConsumer

# List of Kafka topics to subscribe to
topics = 'ipl'

# Kafka broker configuration
bootstrap_servers = ['localhost:9092']

# Kafka consumer group ID
# group_id = 'Gujarati-Dhokla'

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

