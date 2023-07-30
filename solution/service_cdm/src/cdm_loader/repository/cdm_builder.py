from typing import Dict, List

from cdm_loader.repository.cdm_objects import user_category_counters, user_product_counters


class CdmBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict

    def user_category_counters(self) -> List[user_category_counters]:
        user_category_counters_list = []
        for input in self._dict['user_category_counters']:
            user_category_counters_list.append(
                user_category_counters(
                    user_id=input['user_id'],
                    category_id=input['category_id'],
                    category_name=input['category_name'],
                    order_cnt=input['order_cnt']
                )
            )
        return user_category_counters_list
    
    def user_product_counters(self) -> List[user_product_counters]:
        user_product_counters_list = []
        for input in self._dict['user_product_counters']:
            user_product_counters_list.append(
                user_product_counters(
                    user_id=input['user_id'],
                    product_id=input['product_id'],
                    product_name=input['product_name'],
                    order_cnt=input['order_cnt']
                )
            )
        return user_product_counters_list
