from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect.kafka_connectors import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size,
                 logger: Logger,
                 ) -> None:

        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for i in range(self._batch_size):

            message_consume = self._consumer.consume()
            if not message_consume:
                break
         
            self._cdm_repository.user_category_counters_insert(message_consume['user_category_counters'])
            self._cdm_repository.user_product_counters_insert(message_consume['user_product_counters'])

        self._logger.info(f"{datetime.utcnow()}: FINISH")
