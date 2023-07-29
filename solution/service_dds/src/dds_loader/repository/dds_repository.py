import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_category_insert(self,
                            products: List,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.h_category(h_category_pk, category_name,
                                                      load_dt, load_src)
                            values (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                            on conflict (h_category_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src
                            ;
                        """,
                        {
                            'h_category_pk': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                                       product['category']),
                            'category_name': product['category'],
                            'load_dt': load_dt,
                            'load_src': load_src
                        }
                    )

    def h_order_insert(self,
                            order_id: int,
                            order_dt: datetime,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_order(h_order_pk, order_id,
                                                  order_dt, load_dt, load_src)
                        values (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        on conflict (h_order_pk) do update
                        set
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_order_pk': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                                   str(order_id)),
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_product_insert(self,
                            products: List,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.h_product(h_product_pk, product_id,
                                                      load_dt, load_src)
                            values (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                            on conflict (h_product_pk) do update
                            set
                            product_id = EXCLUDED.product_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            'h_product_pk': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                                       product['id']),
                            'product_id': product['id'],
                            'load_dt': load_dt,
                            'load_src': load_src
                        }
                    )

    def h_restaurant_insert(self,
                            restaurant_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_restaurant(h_restaurant_pk, restaurant_id,
                                                  load_dt, load_src)
                        values (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        on conflict (h_restaurant_pk) do update
                        set
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_restaurant_pk': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                                   restaurant_id),
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_user_insert(self,
                            user_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.h_user(h_user_pk, user_id,
                                                  load_dt, load_src)
                        values (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        on conflict (h_user_pk) do update
                        set
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_user_pk': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                                   user_id),
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_order_product_insert(self,
                            order_id: int,
                            products: List,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.l_order_product(hk_order_product_pk, h_order_pk,
                                                           h_product_pk, load_dt, load_src)
                            select 
                            md5(h_order_pk::text || h_product_pk::text)::uuid,
                            h_order_pk,
                            h_product_pk,
                            %(load_dt)s,
                            %(load_src)s
                            from
                            (select h_order_pk from dds.h_order where order_id = %(order_id)s) as ho
                            cross join
                            (select h_product_pk from dds.h_product where product_id = %(product_id)s) as hp
                            on conflict (hk_order_product_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'order_id': order_id,
                            'product_id': product['id'],
                            'load_dt': load_dt,
                            'load_src': load_src
                        }
                    )

    def l_order_user_insert(self,
                            order_id: int,
                            user_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_order_user(hk_order_user_pk, h_order_pk,
                                                       h_user_pk, load_dt, load_src)
                        select 
                        md5(h_order_pk::text || h_user_pk::text)::uuid,
                        h_order_pk,
                        h_user_pk,
                        %(load_dt)s,
                        %(load_src)s
                        from
                        (select h_order_pk from dds.h_order where order_id = %(order_id)s) as ho
                        cross join
                        (select h_user_pk from dds.h_user where user_id = %(user_id)s) as hu
                        on conflict (hk_order_user_pk) do update
                        set
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                            
                        'order_id': order_id,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_product_category_insert(self,
                            products: List,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.l_product_category(hk_product_category_pk, h_product_pk,
                                                           h_category_pk, load_dt, load_src)
                            select 
                            md5(h_product_pk::text || h_category_pk::text)::uuid,
                            h_product_pk,
                            h_category_pk,
                            %(load_dt)s,
                            %(load_src)s
                            from
                            (select h_product_pk from dds.h_product where product_id = %(product_id)s) as hp
                            cross join
                            (select h_category_pk from dds.h_category where category_name = %(category_name)s) as hc
                            on conflict (hk_product_category_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'product_id': product['id'],
                            'category_name': product['category'],
                            'load_dt': load_dt,
                            'load_src': load_src
                        }
                    )

    def l_product_restaurant_insert(self,
                            products: List,
                            restaurant_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.l_product_restaurant(hk_product_restaurant_pk, h_product_pk,
                                                           h_restaurant_pk, load_dt, load_src)
                            select 
                            md5(h_product_pk::text || h_restaurant_pk::text)::uuid,
                            h_product_pk,
                            h_restaurant_pk,
                            %(load_dt)s,
                            %(load_src)s
                            from
                            (select h_product_pk from dds.h_product where product_id = %(product_id)s) as hp
                            cross join
                            (select h_restaurant_pk from dds.h_restaurant where restaurant_id = %(restaurant_id)s) as hr
                            on conflict (hk_product_restaurant_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'product_id': product['id'],
                            'restaurant_id': restaurant_id,
                            'load_dt': load_dt,
                            'load_src': load_src
                        }
                    )

    def s_order_cost_insert(self,
                            order_id: int,
                            cost: float,
                            payment: float,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                        insert into dds.s_order_cost(h_order_pk, cost, payment,
                                                    load_dt, load_src, hk_order_cost_hashdiff)
                        select 
                        h_order_pk,
                        %(cost)s as cost,
                        %(payment)s as payment,
                        %(load_dt)s as load_dt,
                        %(load_src)s as load_src,
                        %(hk_order_cost_hashdiff)s
                        from
                        (select h_order_pk from dds.h_order where order_id = %(order_id)s) as ho
                        on conflict (h_order_pk, load_dt) do update
                        set
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;
                    """,
                    {
                            
                        'order_id': order_id,
                        'cost': cost,
                        'payment': payment,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_order_cost_hashdiff': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                               str(order_id)+str(cost)+str(payment)+
                                               str(load_dt)+str(load_src))
                    }
                )

    def s_order_status_insert(self,
                            order_id: int,
                            status: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                        insert into dds.s_order_status(h_order_pk, status,
                                                    load_dt, load_src, hk_order_status_hashdiff)
                        select 
                        h_order_pk,
                        %(status)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_status_hashdiff)s
                        from
                        (select h_order_pk from dds.h_order where order_id = %(order_id)s) as ho
                        on conflict (h_order_pk, load_dt) do update
                        set
                        status = EXCLUDED.status,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;
                    """,
                    {
                            
                        'order_id': order_id,
                        'status': status,
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_order_status_hashdiff': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                               str(order_id)+str(status)+
                                               str(load_dt)+str(load_src))
                    }
                )

    def s_product_names_insert(self,
                            products: List,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product in products:
                    cur.execute(
                        """
                            insert into dds.s_product_names(h_product_pk, name,
                                                        load_dt, load_src, hk_product_names_hashdiff)
                            select 
                            h_product_pk,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                            from
                            (select h_product_pk from dds.h_product where product_id = %(product_id)s) as hp
                            on conflict (h_product_pk, load_dt) do update
                            set
                            name = EXCLUDED.name,
                            load_src = EXCLUDED.load_src,
                            hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;
                        """,
                        {
                            
                            'product_id': product['id'],
                            'name': product['name'],
                            'load_dt': load_dt,
                            'load_src': load_src,
                            'hk_product_names_hashdiff': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                               str(product['id'])+str(product['name'])+
                                               str(load_dt)+str(load_src))
                        }
                    )

    def s_restaurant_names_insert(self,
                            restaurant: Dict,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_restaurant_names(h_restaurant_pk, name,
                                                    load_dt, load_src, hk_restaurant_names_hashdiff)
                        select 
                        h_restaurant_pk,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                        from
                        (select h_restaurant_pk from dds.h_restaurant where restaurant_id = %(restaurant_id)s) as hr
                        on conflict (h_restaurant_pk, load_dt) do update
                        set
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;
                    """,
                    {
                          
                        'restaurant_id': restaurant['id'],
                        'name': restaurant['name'],
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_restaurant_names_hashdiff': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                               str(restaurant['id'])+str(restaurant['name'])+
                                               str(load_dt)+str(load_src))
                    }
                )

    def s_user_names_insert(self,
                            user: Dict,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_user_names(h_user_pk, username, userlogin,
                                                    load_dt, load_src, hk_user_names_hashdiff)
                        select 
                        h_user_pk,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_user_names_hashdiff)s
                        from
                        (select h_user_pk from dds.h_user where user_id = %(user_id)s) as hu
                        on conflict (h_user_pk, load_dt) do update
                        set
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;
                    """,
                    {
                          
                        'user_id': user['id'],
                        'username': user['name'],
                        'userlogin': user['login'],
                        'load_dt': load_dt,
                        'load_src': load_src,
                        'hk_user_names_hashdiff': uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                               str(user['id'])+str(user['name'])+user['login']+
                                               str(load_dt)+str(load_src))
                    }
                )

    def user_category_counters_get(self):

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select 
                    hu.h_user_pk as user_id,
                    hc.h_category_pk as category_id,
                    hc.category_name as category_name,
                    count(*) as order_cnt
                    from dds.h_order ho
                    left join dds.l_order_user lou on ho.h_order_pk = lou.h_order_pk
                    left join dds.l_order_product lop on ho.h_order_pk = lop.h_order_pk
                    left join dds.l_product_category lpc on lop.h_product_pk = lpc.h_product_pk
                    left join dds.h_user hu on lou.h_user_pk = hu.h_user_pk
                    left join dds.h_category hc on lpc.h_category_pk = hc.h_category_pk
                    group by hu.h_user_pk, hc.h_category_pk, hc.category_name;
                    """                   
                )
                objs = cur.fetchall()
        return objs
    
    def user_product_counters_get(self):

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select 
                    hu.h_user_pk as user_id,
                    hp.h_product_pk as product_id,
                    spn.name as product_name,
                    count(*) as order_cnt
                    from dds.h_order ho
                    left join dds.l_order_user lou on ho.h_order_pk = lou.h_order_pk
                    left join dds.l_order_product lop on ho.h_order_pk = lop.h_order_pk
                    left join dds.h_user hu on lou.h_user_pk = hu.h_user_pk
                    left join dds.h_product hp on lop.h_product_pk = hp.h_product_pk
                    left join dds.s_product_names spn
                    on (hp.h_product_pk = spn.h_product_pk and hp.load_dt = spn.load_dt)
                    group by hu.h_user_pk, hp.h_product_pk, spn.name;
                    """                   
                )
                objs = cur.fetchall()
        return objs
    
    




