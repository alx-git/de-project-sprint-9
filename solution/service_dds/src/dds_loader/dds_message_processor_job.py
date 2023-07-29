import time
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from lib.kafka_connect import KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository 
        self._batch_size = 30
        self._logger = logger

    def run(self) -> None:
        
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(self._batch_size):

            message_consume = self._consumer.consume()
            if not message_consume:
                break
            
            load_dt = datetime.utcnow()
            load_src = 'orders-system-kafka'
            payload = message_consume['payload']

            self._dds_repository.h_category_insert(payload['products'], load_dt, load_src)
            self._dds_repository.h_order_insert(payload['id'], payload['date'], load_dt, load_src)
            self._dds_repository.h_product_insert(payload['products'], load_dt, load_src)
            self._dds_repository.h_restaurant_insert(payload['restaurant']['id'], load_dt, load_src)
            self._dds_repository.h_user_insert(payload['user']['id'], load_dt, load_src)
            self._dds_repository.l_order_product_insert(payload['id'], payload['products'], 
                                                         load_dt, load_src)
            
            self._dds_repository.l_order_user_insert(payload['id'], payload['user']['id'],
                                                     load_dt, load_src)
            
            self._dds_repository.l_product_category_insert(payload['products'], load_dt, load_src)
            
            self._dds_repository.l_product_restaurant_insert(payload['products'], payload['restaurant']['id'],
                                                             load_dt, load_src)
            
            self._dds_repository.s_order_cost_insert(payload['id'], payload['cost'], 
                                                     payload['payment'], load_dt, load_src)
            
            self._dds_repository.s_order_status_insert(payload['id'], payload['status'],
                                                       load_dt, load_src)
            
            self._dds_repository.s_product_names_insert(payload['products'], load_dt, load_src)
            self._dds_repository.s_restaurant_names_insert(payload['restaurant'], load_dt, load_src)
            self._dds_repository.s_user_names_insert(payload['user'], load_dt, load_src)
            
            message_produce = {
                    'user_category_counters': self.collect_user_category_counters(),
                    'user_product_counters': self.collect_user_product_counters()
                    }
            self._producer.produce(message_produce)

        time.sleep(2)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def collect_user_category_counters(self):
        user_category_counters_list = []
        user_category_counters = self._dds_repository.user_category_counters_get()
        for user_category_counter in user_category_counters:
            user_category_counters_list.append({'user_id': str(user_category_counter[0]),
                                                'category_id': str(user_category_counter[1]),
                                                'category_name': user_category_counter[2],
                                                'order_cnt': user_category_counter[3]})
        return user_category_counters_list
    
    def collect_user_product_counters(self):
        user_product_counters_list = []
        user_product_counters = self._dds_repository.user_product_counters_get()
        for user_product_counter in user_product_counters:
            user_product_counters_list.append({'user_id': str(user_product_counter[0]),
                                                'product_id': str(user_product_counter[1]),
                                                'product_name': user_product_counter[2],
                                                'order_cnt': user_product_counter[3]})
        return user_product_counters_list


