import os, base64, re, logging
from elasticsearch import Elasticsearch

from flask import Flask,render_template
from asgiref.wsgi import WsgiToAsgi


app = Flask(__name__)

# Log transport details (optional):

logging.basicConfig(level=logging.INFO)

# Parse the auth and host from env:

#bonsai = os.environ["https://i1gd3mbo4r:sf8q52fulp@hn-search-9858436033.us-east-1.bonsaisearch.net:443"]
# bonsai = str("https://i1gd3mbo4r:sf8q52fulp@hn-search-9858436033.us-east-1.bonsaisearch.net:443")
# auth = re.search('https\:\/\/(.*)\@', bonsai).group(1).split(':')
# host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')

# # Optional port

# match = re.search('(:\d+)', host)
# if match:
#     p = match.group(0)
#     host = host.replace(p, '')
#     port = int(p.split(':')[1])
# else:
#     port=443

# Connect to cluster over SSL using auth for best security:

# es_header = [{
# 'host': host,
# 'port': port,
# 'use_ssl': True,
# 'http_auth': (auth[0],auth[1])
# }]

es_header = [{
'host': 'https://hn-search-9858436033.us-east-1.bonsaisearch.net',
'port': 443,
'use_ssl': True,
'http_auth': ("i1gd3mbo4r","sf8q52fulp")
}]
# Create the client instance
# Instantiate the new Elasticsearch connection:
#es = Elasticsearch(es_header)

client = Elasticsearch(
        ['https://hn-search-9858436033.us-east-1.bonsaisearch.net:443'],
        http_auth=("t2mABiEPJn","KwyXmSUap5AMfcR34Lvi"))

@app.route("/")
def index():
    # Successful response!
    #client.info()
    return client.info()


asgi_app = WsgiToAsgi(app)