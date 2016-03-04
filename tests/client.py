#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import os
import sys

sys.path.append(os.getcwd())

import struct

from tornado import ioloop
from tornado import gen
from tornado import tcpclient

from configs import db
from configs.codec import *


@gen.coroutine
def register_dbname(dbname):



@gen.coroutine
def main():
    client = tcpclient.TCPClient()
    stream = yield client.connect("127.0.0.1", 32000)
    payload = b""
    # db name
    dbname_bytes = serialize_value(db.db)
    payload += dbname_bytes
    # table name
    tname_bytes = serialize_value("users")
    payload += tname_bytes
    # op columns
    # # gem
    op_gem_colname_bytes = serialize_value("gem")
    payload += op_gem_colname_bytes
    # # payload opcode
    payload += bytes([popc_add])
    # # gem value
    op_gem_colvalue_bytes = serialize_value(1)
    payload += op_gem_colvalue_bytes
    # separator
    payload += bytes([vt_space])
    # match columns
    # # uid
    match_uid_colname_bytes = serialize_value("uid")
    payload += match_uid_colname_bytes
    # # match type
    payload += bytes([match_equal])
    # # uid value
    match_uid_colvalue_bytes = serialize_value(1)
    payload += match_uid_colvalue_bytes
    # # match relation
    payload += bytes([match_and])
    header = struct.pack(">LB", len(payload), opc_payload).ljust(10)
    yield stream.write(header+payload)

if __name__ == "__main__":
    io_loop = ioloop.IOLoop.current()
    io_loop.run_sync(main)
