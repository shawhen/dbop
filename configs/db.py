#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

from .switches import *

import drivers


driver = drivers.driver_redis

host = "redis.qd.shawhen.me"
port = 6379
user = "root"
passwd = "MyMac2015"

__dbbase__ = "wealthempire"
if develop is True:
    db = __dbbase__ + "_develop"
elif debug is True:
    db = __dbbase__ + "_debug"
else:
    db = __dbbase__

if driver == drivers.driver_mysql:
    from tornado_mysql import pools

    pool = pools.Pool({"host": host, "port": port, "user": user, "passwd": passwd, "db": db})
elif driver == drivers.driver_redis:
    import redis

    red = redis.Redis(host, port)

else:
    raise NotImplemented
