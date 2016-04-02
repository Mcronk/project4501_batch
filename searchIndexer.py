#Takes new listings out of Kafka and 
#places them into elasticSearch
import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

def get_message_indexing():
	consumer = KafkaConsumer('course-topic', group_id='course-indexer', bootstrap_servers=['kafka:9092'])
	for message in consumer:
		new_listing = json.loads((message.value).decode('utf-8'))
		
		es = Elasticsearch(['es'])
		# some_new_listing = {'title': 'Used MacbookAir 13"', 'description': 'This is a used Macbook Air in great condition', 'id':42}
		es.index(index='listing_index', doc_type='listing', id=new_listing['id'], body=new_listing)
		es.indices.refresh(index="listing_index")
		# es.search(index='listing_index', body={'query': {'query_string': {'query': 'macbook air'}}, 'size': 10})

def search_result():
	# es.search(index='listing_index', body={'query': {'query_string': {'query': 'macbook air'}}, 'size': 10})

time.sleep(5)
get_message_indexing()
