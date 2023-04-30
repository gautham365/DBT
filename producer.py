from time import sleep
from json import dumps
import kafka
import socket
from json import loads
import json

# define host and port for socket server
host = '127.0.0.1'
port = 5558

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((host, port))
server.listen(5)

# kafka producer
producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

while True:
    data = ''  # Initialize the variable outside the loop
    
    #wait till data is received
    client, address = server.accept()
    msg = client.recv(1024)
    if len(msg) <= 0:
        break
    data += msg.decode("utf-8")
    # print("message received",data)
    #convert to dictionary 
    val=json.loads(data)
    
    topic=val['topic']
    data=val['data']
    print(topic)
    #send to broker
    producer.send(topic, value=data)


