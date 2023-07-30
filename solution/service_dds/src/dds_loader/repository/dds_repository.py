from typing import List

from lib.pg import PgConnect
from dds_loader.repository.dds_objects import h_category, h_order, h_product, h_restaurant, h_user,\
                        l_order_product, l_order_user, l_product_category,\
                        l_product_restaurant, s_order_cost, s_order_status,\
                        s_product_names, s_restaurant_names, s_user_names


class DdsRepository():
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_category_insert(self, categories:List[h_category]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for category in categories:
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
                            'h_category_pk': category.h_category_pk,
                            'category_name': category.category_name,
                            'load_dt': category.load_dt,
                            'load_src': category.load_src
                        }
                    )

    def h_order_insert(self, order:h_order) -> None:

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
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )

    def h_product_insert(self, products:List[h_product]) -> None:

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
                            'h_product_pk': product.h_product_pk,
                            'product_id': product.product_id,
                            'load_dt': product.load_dt,
                            'load_src': product.load_src
                        }
                    )

    def h_restaurant_insert(self, restaurant:h_restaurant) -> None:

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
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )

    def h_user_insert(self, user:h_user) -> None:

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
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def l_order_product_insert(self, order_products:List[l_order_product]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for order_product in order_products:
                    cur.execute(
                        """
                            insert into dds.l_order_product(hk_order_product_pk, h_order_pk,
                                                           h_product_pk, load_dt, load_src)
                            values (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s,
                                    %(load_dt)s, %(load_src)s)
                            on conflict (hk_order_product_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'hk_order_product_pk': order_product.hk_order_product_pk,
                            'h_order_pk': order_product.h_order_pk,
                            'h_product_pk': order_product.h_product_pk,
                            'load_dt': order_product.load_dt,
                            'load_src': order_product.load_src
                        }
                    )

    def l_order_user_insert(self, order_user: l_order_user) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.l_order_user(hk_order_user_pk, h_order_pk,
                                                       h_user_pk, load_dt, load_src)
                        values (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s,
                                    %(load_dt)s, %(load_src)s)
                        on conflict (hk_order_user_pk) do update
                        set
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                            
                        'hk_order_user_pk': order_user.hk_order_user_pk,
                        'h_order_pk': order_user.h_order_pk,
                        'h_user_pk': order_user.h_user_pk,
                        'load_dt': order_user.load_dt,
                        'load_src': order_user.load_src
                    }
                )

    def l_product_category_insert(self, product_categories:List[l_product_category]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product_category in product_categories:
                    cur.execute(
                        """
                            insert into dds.l_product_category(hk_product_category_pk, h_product_pk,
                                                           h_category_pk, load_dt, load_src)
                            values (%(hk_product_category_pk)s, %(h_product_pk)s, %(h_category_pk)s,
                                    %(load_dt)s, %(load_src)s)
                            on conflict (hk_product_category_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'hk_product_category_pk': product_category.hk_product_category_pk,
                            'h_product_pk': product_category.h_product_pk,
                            'h_category_pk': product_category.h_category_pk,
                            'load_dt': product_category.load_dt,
                            'load_src': product_category.load_src
                        }
                    )

    def l_product_restaurant_insert(self, product_restaurants:List[l_product_restaurant]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product_restaurant in product_restaurants:
                    cur.execute(
                        """
                            insert into dds.l_product_restaurant(hk_product_restaurant_pk, h_product_pk,
                                                           h_restaurant_pk, load_dt, load_src)
                            values (%(hk_product_restaurant_pk)s, %(h_product_pk)s, %(h_restaurant_pk)s,
                                    %(load_dt)s, %(load_src)s)
                            on conflict (hk_product_restaurant_pk) do update
                            set
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                        """,
                        {
                            
                            'hk_product_restaurant_pk': product_restaurant.hk_product_restaurant_pk,
                            'h_product_pk': product_restaurant.h_product_pk,
                            'h_restaurant_pk': product_restaurant.h_restaurant_pk,
                            'load_dt': product_restaurant.load_dt,
                            'load_src': product_restaurant.load_src
                        }
                    )

    def s_order_cost_insert(self, order_cost:s_order_cost) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                        insert into dds.s_order_cost(h_order_pk, cost, payment,
                                                    load_dt, load_src, hk_order_cost_hashdiff)
                        values (%(h_order_pk)s, %(cost)s, %(payment)s,
                                    %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
                        on conflict (h_order_pk, load_dt) do update
                        set
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;
                    """,
                    {
                            
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff 
                    }
                )

    def s_order_status_insert(self, order_status:s_order_status) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                
                cur.execute(
                    """
                        insert into dds.s_order_status(h_order_pk, status,
                                                    load_dt, load_src, hk_order_status_hashdiff)
                        values (%(h_order_pk)s, %(status)s,
                                    %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
                        on conflict (h_order_pk, load_dt) do update
                        set
                        status = EXCLUDED.status,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;
                    """,
                    {
                            
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )

    def s_product_names_insert(self, product_names:List[s_product_names]) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for product_name in product_names:
                    cur.execute(
                        """
                            insert into dds.s_product_names(h_product_pk, name,
                                                        load_dt, load_src, hk_product_names_hashdiff)
                            values (%(h_product_pk)s, %(name)s,
                                    %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
                            on conflict (h_product_pk, load_dt) do update
                            set
                            name = EXCLUDED.name,
                            load_src = EXCLUDED.load_src,
                            hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;
                        """,
                        {
                            
                            'h_product_pk': product_name.h_product_pk,
                            'name': product_name.name,
                            'load_dt': product_name.load_dt,
                            'load_src': product_name.load_src,
                            'hk_product_names_hashdiff': product_name.hk_product_names_hashdiff
                        }
                    )

    def s_restaurant_names_insert(self, restaurant_name: s_restaurant_names) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_restaurant_names(h_restaurant_pk, name,
                                                    load_dt, load_src, hk_restaurant_names_hashdiff)
                        values (%(h_restaurant_pk)s, %(name)s,
                                    %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
                        on conflict (h_restaurant_pk, load_dt) do update
                        set
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;
                    """,
                    {
                          
                        'h_restaurant_pk': restaurant_name.h_restaurant_pk,
                        'name': restaurant_name.name,
                        'load_dt': restaurant_name.load_dt,
                        'load_src': restaurant_name.load_src,
                        'hk_restaurant_names_hashdiff': restaurant_name.hk_restaurant_names_hashdiff
                    }
                )

    def s_user_names_insert(self, user_name:s_user_names) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into dds.s_user_names(h_user_pk, username, userlogin,
                                                    load_dt, load_src, hk_user_names_hashdiff)
                        values (%(h_user_pk)s, %(username)s, %(userlogin)s,
                                    %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
                        on conflict (h_user_pk, load_dt) do update
                        set
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;
                    """,
                    {
                          
                        'h_user_pk': user_name.h_user_pk,
                        'username': user_name.username,
                        'userlogin': user_name.userlogin,
                        'load_dt': user_name.load_dt,
                        'load_src': user_name.load_src,
                        'hk_user_names_hashdiff': user_name.hk_user_names_hashdiff
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