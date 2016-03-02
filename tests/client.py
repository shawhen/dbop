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


# --------------- opcode[1 byte] -------------
opc_register_dbname = 0
opc_register_tablename = 1
opc_register_columnname = 2

opc_payload = 101

# ------------ payload opcode[2 bytes] ---------------
# basic algorithm
popc_add = 0
popc_sub = 1
popc_multi = 2
popc_divide = 3
popc_mod = 4

# for string/binary
popc_append = 101
popc_prepend = 102
popc_replace = 103

# for list
popc_extend = 201
popc_remove = 202

# -------------- value type ---------
vt_integer = 0
vt_text = 1  # utf-8 encoded
vt_binary = 2

vt_list = 3

vt_space = 255

# -------------- match type -------------
match_equal = 0  # =
match_not_equal = 1  # !=
match_greater = 2  # >
match_lesser = 3  # <
match_greater_equal = 4  # >=
match_lesser_equal = 5  # <=

# ------------- match relation -----------
match_and = 0
match_or = 1

def parse_value(data, baseoffset=0):
    """
    data should be klv(data value type, data value length, data value)
    :param data:
    :return: (value, consumed bytes)
    """
    if baseoffset >= len(data):
        return None, 0
    data = data[baseoffset:]
    value_len = struct.unpack(">H", data[1:3])[0]
    if data[0] == vt_integer:
        value = struct.unpack(">Q", data[3:3+value_len].rjust(8))[0]
    elif data[0] == vt_text:
        value = data[3:3+value_len].decode()
    elif data[0] == vt_binary:
        value = data[3:3+value_len]
    elif data[0] == vt_list:
        value = []
        baseoffseti = 3
        while True:
            valuei, consumed = parse_value(data, baseoffseti)
            baseoffseti += consumed
            if valuei is None:
                break
            value.append(valuei)
    elif data[0] == vt_space:
        value = None

        return value, 1
    else:
        raise NotImplemented
    return value, 3+value_len


def serialize_value(value):
    if isinstance(value, int):
        # todo: more tight here
        return bytes([vt_integer])+struct.pack(">HQ", 8, value)
    elif isinstance(value, str):
        encoded_value = value.encode()
        return bytes([vt_text])+struct.pack(">H", len(encoded_value))+encoded_value
    elif isinstance(value, bytes):
        return bytes([vt_binary])+struct.pack(">H", len(value))+value
    elif isinstance(value, list):
        vt_bytes = bytes([vt_list])
        vv_bytes = b""
        for valuei in value:
            valuebytes = serialize_value(valuei)
            vv_bytes += valuebytes
        return vt_bytes+struct.pack(">H", len(vv_bytes))+vv_bytes
    else:
        raise NotImplemented


@gen.coroutine
def main():
    client = tcpclient.TCPClient()
    stream = yield client.connect("127.0.0.1", 32000)
    payload = b""
    # dbnamen
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
