import uuid

from datetime import datetime
from typing import Any, Dict, List

from dds_loader.repository.dds_objects import h_category, h_order, h_product, h_restaurant, h_user,\
                                              l_order_product, l_order_user, l_product_category,\
                                              l_product_restaurant, s_order_cost, s_order_status,\
                                              s_product_names, s_restaurant_names, s_user_names


class DdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = 'orders_system_kafka'
        self.datetime = datetime.utcnow()
        self.order_ns_uuid = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_category(self) -> List[h_category]:
        categories = []
        for prod_dict in self._dict['products']:
            category_name = prod_dict['category']
            categories.append(
                h_category(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=self.datetime,
                    load_src=self.source_system
                )
            )
        return categories
    
    def h_order(self) -> h_order:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return h_order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=self.datetime,
            load_src=self.source_system
        )

    def h_product(self) -> List[h_product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                h_product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=self.datetime,
                    load_src=self.source_system
                )
            )
        return products
    
    def h_restaurant(self) -> h_restaurant:
        restaurant_id = self._dict['restaurant']['id']
        return h_restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=self.datetime,
            load_src=self.source_system
        )

    def h_user(self) -> h_user:
        user_id = self._dict['user']['id']
        return h_user(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=self.datetime,
            load_src=self.source_system
        )
    
    def l_order_product(self) -> List[l_order_product]:
        order_products = []
        order_id = self._dict['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            order_products.append(
                l_order_product(
                    hk_order_product_pk=self._uuid(str(order_id)+prod_id),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=self.datetime,
                    load_src=self.source_system
                )
            )
        return order_products
    
    def l_order_user(self) -> l_order_user:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return l_order_user(
            hk_order_user_pk=self._uuid(str(order_id)+user_id),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=self.datetime,
            load_src=self.source_system
        )
    
    def l_product_category(self) -> List[l_product_category]:
        product_categories = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            category_name = prod_dict['category']
            product_categories.append(
                l_product_category(
                    hk_product_category_pk=self._uuid(prod_id+category_name),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(category_name),
                    load_dt=self.datetime,
                    load_src=self.source_system
                )
            )
        return product_categories
    
    def l_product_restaurant(self) -> List[l_product_restaurant]:
        product_restaurants = []
        restaurant_id = self._dict['restaurant']['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            product_restaurants.append(
                l_product_restaurant(
                    hk_product_restaurant_pk=self._uuid(prod_id+restaurant_id),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=self.datetime,
                    load_src=self.source_system
                )
            )
        return product_restaurants
    
    def s_order_cost(self) -> s_order_cost:
        order_id = self._dict['id']
        cost = self._dict['cost']
        payment = self._dict['payment']
        return s_order_cost(
            h_order_pk=self._uuid(order_id),
            cost=cost,
            payment=payment,
            load_dt=self.datetime,
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(str(self._uuid(order_id))+str(cost)+str(payment)+
                                              str(self.datetime)+self.source_system)
        )
    
    def s_order_status(self) -> s_order_status:
        order_id = self._dict['id']
        status = self._dict['status']
        return s_order_status(
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=self.datetime,
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(str(self._uuid(order_id))+status+
                                              str(self.datetime)+self.source_system)
        )

    def s_product_names(self) -> List[s_product_names]:
        product_names = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            name = prod_dict['name']
            product_names.append(
                s_product_names(
                    h_product_pk=self._uuid(prod_id),
                    name=name,
                    load_dt=self.datetime,
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(str(self._uuid(prod_id))+name+
                                              str(self.datetime)+self.source_system)
                )
            )
        return product_names
    
    def s_restaurant_names(self) -> s_restaurant_names:
        restaurant_id = self._dict['restaurant']['id']
        name = self._dict['restaurant']['name']
        return s_restaurant_names(
            h_restaurant_pk=self._uuid(restaurant_id),
            name=name,
            load_dt=self.datetime,
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(str(self._uuid(restaurant_id))+name+
                                              str(self.datetime)+self.source_system)
        )
    
    def s_user_names(self) -> s_user_names:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return s_user_names(
            h_user_pk=self._uuid(user_id),
            username=username,
            userlogin=userlogin,
            load_dt=self.datetime,
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(str(self._uuid(user_id))+username+userlogin+
                                              str(self.datetime)+self.source_system)
        )    