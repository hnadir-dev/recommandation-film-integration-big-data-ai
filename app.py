import logging
from elasticsearch import Elasticsearch

from flask import Flask
from asgiref.wsgi import WsgiToAsgi
import json


app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

client = Elasticsearch(
        ['https://hn-search-9858436033.us-east-1.bonsaisearch.net:443'],
        http_auth=("t2mABiEPJn","KwyXmSUap5AMfcR34Lvi"))


@app.route("/")
def index():
    # Successful response!
    #spark = create_spark_configuration()
    return 'Api'

@app.route("/movies/<int:index>")
def movies(index):
    jsf = open('data/json/movies.json','r+')
    data = json.load(jsf)

    movies = data['results']

    if index > data['count']:
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return movies[index]

@app.route("/users/<int:index>")
def users(index):
    jsf = open('data/json/users.json','r+')
    data = json.load(jsf)

    users = data['results']

    if index > data['count']:    
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return users[index]

@app.route("/ratings/<int:index>")
def ratings(index):
    jsf = open('data/json/ratings.json','r+')
    data = json.load(jsf)

    ratings = data['results']

    if index > data['count']:    
        return dict({'message':'Entre a valide index!','error_id': 404})
    else:
        return ratings[index]


if __name__ == '__main__':
    app.run(debug=True)
#asgi_app = WsgiToAsgi(app)