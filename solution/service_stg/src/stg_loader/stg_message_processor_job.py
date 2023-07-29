import json
import time
from datetime import datetime
from logging import Logger

from lib.kafka_connect.kafka_connectors import KafkaConsumer
from lib.kafka_connect.kafka_connectors import KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = 100
        self._logger = logger

    
    def run(self) -> None:
    
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(self._batch_size):
            message_consume = self._consumer.consume()
            if not message_consume:
                break
            payload = message_consume['payload']
            self._stg_repository.order_events_insert(message_consume['object_id'],
                                                     message_consume['object_type'],
                                                     message_consume['sent_dttm'],
                                                     json.dumps(payload))
           
            user = self._redis.get(payload['user']['id'])
            restaurant = self._redis.get(payload['restaurant']['id'])
            message_produce = {
                    'object_id': message_consume['object_id'],
                    'object_type': 'order',
                    'payload': {
                        'id': message_consume['object_id'],
                        'date': payload['date'],
                        'cost': payload['cost'],
                        'payment': payload['payment'],
                        'status': payload['final_status'],
                        'restaurant': {'id': restaurant['_id'], 'name': restaurant['name']},
                        'user': {'id': user['_id'], 'name': user['name'],
                                'login': user['login']},
                        'products': self.collect_products(payload['order_items'], restaurant)
                    }
                }
            self._producer.produce(message_produce)

        time.sleep(2)
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def collect_products(self, order_items, restaurant):
        product_list = []
        for order_item in order_items:
            for menu_item in restaurant['menu']:
                if order_item['id'] == menu_item['_id']:
                    product_list.append({'id': order_item['id'],
                                        'price': order_item['price'],
                                        'quantity': order_item['quantity'],
                                        'name': menu_item['name'],
                                        'category': menu_item['category']})
                    break
        return product_list


