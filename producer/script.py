from confluent_kafka import Producer
import requests
import json
import logging
import time

movies_topic = "movies-topic"
users_topic = "users-topic"
ratings_topic = "ratings-topic"

kafka_config = {
    "bootstrap.servers": "localhost:9092",
}

# Configure logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

producer = Producer(kafka_config)


moviesCount = 0
usersCount = 0
ratingCount = 0

moviesApiUrl = "http://127.0.0.1:5000/movies/{}"
usersApiUrl = "http://127.0.0.1:5000/users/{}"
ratingsApiUrl = "http://127.0.0.1:5000/ratings/{}"

if __name__ == '__main__':

    while True:
        movies_response = requests.get(moviesApiUrl.format(moviesCount))
        users_response = requests.get(usersApiUrl.format(usersCount))
        ratings_response = requests.get(ratingsApiUrl.format(ratingCount))

        # if "error_id" not in movies_response.json():
        #     print('***************** Movie ******************')
        #     producer.produce(movies_topic, key="key", value=json.dumps(movies_response.json()).encode('utf-8'))
        #     producer.flush()
        #     moviesCount = moviesCount + 1
        # else:
        #     moviesCount = 0

        # time.sleep(0.5)

        # if "error_id" not in users_response.json():
        #     print('***************** User ******************')
        #     producer.produce(users_topic, key="key", value=json.dumps(users_response.json()).encode('utf-8'))
        #     producer.flush()
        #     usersCount = usersCount + 1
        # else:
        #     usersCount = 0

        # time.sleep(0.5)

        if "error_id" not in ratings_response.json():
            print('***************** Ratings ******************')
            producer.produce(ratings_topic, key="key", value=json.dumps(ratings_response.json()).encode('utf-8'))
            producer.flush()
            ratingCount = ratingCount + 1
        else:
            ratingCount = 0
        
        time.sleep(0.1)