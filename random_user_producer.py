from kafka import KafkaProducer
import requests
import json
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'randomuser-topic'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_user_data():
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    if response.status_code == 200:
        #x=response.json()
        #print("ACtual data is :"x['results'][0])
        return response.json()['results'][0]
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

def format_data(data):
    res = {}
    location = data['location']
    res['first_name'] = data['name']['first']
    res['last_name'] = data['name']['last']
    res['gender'] = data['gender']
    res['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                     f"{location['city']}, {location['state']}, {location['country']} - {location['postcode']}"
    res['email'] = data['email']
    res['uuid'] = data['login']['uuid']
    res['username'] = data['login']['username']
    res['password'] = data['login']['password']
    res['age'] = data['dob']['age']
    res['phone'] = data.get('phone')
    res['cell'] = data.get('cell')
    res['registered_date'] = data.get('registered', {}).get('date')
    res['registered_age'] = data.get('registered', {}).get('age')
    res['id_name'] = data.get('id', {}).get('name')
    res['id_value'] = data.get('id', {}).get('value')
    picture = data.get('picture', {})
    res['picture_large'] = picture.get('large')
    res['picture_medium'] = picture.get('medium')
    res['picture_thumbnail'] = picture.get('thumbnail')

    return res

def produce_messages():
    while 1:

        data = fetch_user_data()
        if data:
            raw_data = format_data(data)
            print("Formated data is")
            print(json.dumps(raw_data, indent=4))
            producer.send(TOPIC_NAME, value=raw_data)
            print("Sent data to Kafka:", raw_data)
        time.sleep(5)
if __name__ == "__main__":
    try:
        produce_messages()
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        producer.close()
