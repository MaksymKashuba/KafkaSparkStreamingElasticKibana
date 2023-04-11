from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import time
import random
print('Kafka Producer has been initiated...')


brokers = {'localhost:9092'}
kafkaTopic = 'KafkaHW'
speakerN = ['Speaker1', 'Speaker2', 'Speaker3', 'Speaker4', 'Speaker5']
dict_path = 'C:/Users/max19/PycharmProjects/KafkaHW/dictionary.txt'
firstTime = datetime.now()
p = KafkaProducer(
    bootstrap_servers=brokers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    api_version=(0,11,5)
)
speakerRange = [i for i in speakerN]
for i in range(len(speakerN)):
    data = {
        'speaker': random.choice(speakerRange),
        'time': str(firstTime),
        'word': random.choice([x.replace('\n', '') for x in open(dict_path).readlines()])
    }
    speakerRange.remove(data['speaker'])
    print(data)
    data = json.dumps(data)
    p.send(kafkaTopic, value=data)
    time.sleep(3)
for i in range(9999):
    data = {
       'speaker': random.choice(speakerN),
       'time': str(firstTime + timedelta(seconds=10*(i + 1))),
       'word': random.choice([x.replace('\n', '') for x in open(dict_path).readlines()])
       }
    print(data)
    p.send(kafkaTopic, value=data)
    time.sleep(3)


