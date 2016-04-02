#Takes new listings out of Kafka and 
#places them into elasticSearch
import json
import time
import kafka
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

def get_message_indexing():
	consumer = KafkaConsumer('new-course-topic', group_id='listing-indexer', bootstrap_servers=['kafka:9092'])

	for message in consumer:
		new_listing = json.loads((message.value).decode('utf-8'))
		es = Elasticsearch(['es'])
		es.index(index='listing_index', doc_type='listing', id=new_listing['name'], body=new_listing)
		es.indices.refresh(index="listing_index")

# print('batch running and go to sleep for 10 sec')
# print(time.ctime())
time.sleep(15)
# print(time.ctime())
get_message_indexing()
