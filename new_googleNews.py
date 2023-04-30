from newsapi import NewsApiClient
import pymongo
import threading
import json
import time
import datetime

import socket

# define host and port for socket server
host = '127.0.0.1'
port = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((host, port))
server.listen(5)

list_of_clients = []

# Sending Messages To All Connected Clients
def broadcast(message):
    for client in list_of_clients:
        client.sendall(message)
        
        # client.send()

# Set up NewsAPI client
api_key = 'fb51efb9af5e4096b409b458869fe708'
newsapi = NewsApiClient(api_key=api_key)

# # Set up MongoDB client and database
# client = pymongo.MongoClient('mongodb://localhost:27017/dbt')
# #print(client.server_info())
# db = client['dbt']

# Define search queries
queries = ['ipl', 'tesla', 'election']

day=1

def getNews(query):
    list_news = []
    # Make API request and get JSON response
    now = datetime.datetime.now()
    yesterday = now - datetime.timedelta(days=day)
    nextday = now - datetime.timedelta(days=day-1)
    print(yesterday)
    response = newsapi.get_everything(q=query, language='en', sort_by='publishedAt', from_param=yesterday, to=nextday, page_size=1)

    # Parse JSON response and extract relevant data
    articles = response['articles']
    list_news = []
    for article in articles:
        title = article['title']
        url = article['url']
        content = article['content']

        # Create a dictionary object
        data = {'query': query, 'title': title, 'url': url, 'content': content}

        # Append the dictionary object to the list
        list_news.append(data)
    return list_news

def startApp():
    while True:
        global day
        # Iterate over the queries and get news articles for each query
        for query in queries:
            # Get news articles for the current query
            list_news = { "topic": query, "data": getNews(query)}
            # list_news = { "topic": query, "data": [{"title": "HI", "content":"hello"},{"title": "11", "content":"22"}]}

            # Send list_news to the socket server
            # message = json.dumps(list_news)
            message = str(json.dumps(list_news))+"\n"
            # broadcast("hello\n".encode('utf-8'))
            broadcast(message.encode('utf-8'))

            # Wait for some time before making the next API request
            time.sleep(5)
        day += 1


# Receiving / Listening Function
def receive():
    
    while True:
        # Accept Connection
        client, address = server.accept()
        list_of_clients.append(client)

        thread = threading.Thread(target=startApp, args=())
        thread.start()
        
    
receive()
