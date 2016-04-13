#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

from .switches import *

import drivers


driver = drivers.driver_redis

host = "localhost"
port = 6379
user = "root"
passwd = "password"

__dbbase__ = "database"
if develop is True:
    db = __dbbase__ + "_develop"
elif debug is True:
    db = __dbbase__ + "_debug"
else:
    db = __dbbase__

red = None


def init():
    global red

    if driver == drivers.driver_mysql:
        from tornado_mysql import pools

        pool = pools.Pool({"host": host, "port": port, "user": user, "passwd": passwd, "db": db})
    elif driver == drivers.driver_redis:
        import redis

        red = redis.Redis(host, port)

    else:
        raise NotImplementedError("unsupported driver:", driver)
