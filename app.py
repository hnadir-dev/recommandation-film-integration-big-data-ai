import findspark
findspark.init()

import os, base64, re, logging
from elasticsearch import Elasticsearch

from flask import Flask,render_template
from asgiref.wsgi import WsgiToAsgi

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())

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

def create_spark_configuration():
    spark_config = None

    try:
        spark_config = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.13.4")#7.17.14
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())
        
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_config

@app.route("/")
def index():
    # Successful response!
    spark = create_spark_configuration()
    return spark


asgi_app = WsgiToAsgi(app)