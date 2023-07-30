from typing import List

from lib.pg.pg_connect import PgConnect
from cdm_loader.repository.cdm_objects import user_category_counters, user_product_counters


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self,
                                      user_category_counters: List[user_category_counters]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for user_category_counter in user_category_counters:
                                        
                    cur.execute(
                        """
                            insert into cdm.user_category_counters(user_id, category_id,
                                                      category_name, order_cnt)
                            values (%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
                            on conflict (user_id, category_id) do update
                            set
                            category_name = EXCLUDED.category_name,
                            order_cnt = EXCLUDED.order_cnt;
                        """,
                        {
                            'user_id': user_category_counter.user_id,
                            'category_id': user_category_counter.category_id,
                            'category_name': user_category_counter.category_name,
                            'order_cnt': user_category_counter.order_cnt
                        }
                    )

    def user_product_counters_insert(self,
                                     user_product_counters: List[user_product_counters]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for user_product_counter in user_product_counters:
                    cur.execute(
                        """
                            insert into cdm.user_product_counters(user_id, product_id,
                                                      product_name, order_cnt)
                            values (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
                            on conflict (user_id, product_id) do update
                            set
                            product_name = EXCLUDED.product_name,
                            order_cnt = EXCLUDED.order_cnt;
                        """,
                        {
                            'user_id': user_product_counter.user_id,
                            'product_id': user_product_counter.product_id,
                            'product_name': user_product_counter.product_name,
                            'order_cnt': user_product_counter.order_cnt
                        }
                    )
