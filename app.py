import findspark
findspark.init()

import os, base64, re, logging
from elasticsearch import Elasticsearch

from flask import Flask,render_template
from asgiref.wsgi import WsgiToAsgi


app = Flask(__name__)


logging.basicConfig(level=logging.INFO)

es_header = [{
'host': 'https://hn-search-9858436033.us-east-1.bonsaisearch.net',
'port': 443,
'use_ssl': True,
'http_auth': ("i1gd3mbo4r","sf8q52fulp")
}]

client = Elasticsearch(
        ['https://hn-search-9858436033.us-east-1.bonsaisearch.net:443'],
        http_auth=("t2mABiEPJn","KwyXmSUap5AMfcR34Lvi"))

@app.route("/")
def index():
    # Successful response!

    return client.info()


asgi_app = WsgiToAsgi(app)