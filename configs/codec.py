#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import struct

# --------------- errno -----------------
errno_dbname_not_found = 11
errno_tablename_not_found = 12
errno_column_not_found = 13
errno_unsupported_data_type = 14

# --------------- opcode[1 byte] -------------
opc_register_dbname = 0
opc_register_tablename = 1
opc_register_columnname = 2

opc_update = 101
opc_get = 102
opc_delete = 103

# ------------ payload opcode[2 bytes] ---------------
# basic algorithm
popc_add = 0
popc_sub = 1
popc_multi = 2
popc_divide = 3
popc_mod = 4
popc_assign = 10

# for string/binary
popc_append = 101
popc_prepend = 102
popc_replace = 103

# for list
popc_extend = 201  # append some values at the tail
popc_remove = 202  # remove a value from left2right

# for set
popc_sadd = 51  # add some values into the set
popc_sremove = 52  # remove some values from the set

# -------------- protocol value type ---------
vt_integer = 0
vt_text = 1  # utf-8 encoded
vt_binary = 2

vt_list = 3
vt_float = 4

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

# ------------column data store type -----------
# bare store, e.g. number, string
# can be op by redis's
#           set/get/mset/mget/setrange/getrange
#           incr/incrby/decr/decrby
#           append
col_stype_bare = 0
# hash store, e.g. number, string
# can be op by redis's
#           hset/hget/hmset/hmget
# *****     hincrby/hincrbyfloat
col_stype_hash = 1
# list store
# can be op by redis's
#           lpush/rpush/lrange/lrem/ltrim/lset/linsert/lindex
col_stype_list = 2
# set store
# can be op by redis's
#           sadd/srem
col_stype_set = 3
# sorted set store
col_stype_sset = 4


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
        value = struct.unpack(">q", data[3:3+value_len].rjust(8))[0]
    elif data[0] == vt_text:
        value = data[3:3+value_len].decode()
    elif data[0] == vt_binary:
        value = data[3:3+value_len]
    elif data[0] == vt_list:
        value = []
        baseoffseti = 3
        while baseoffseti < value_len:
            valuei, consumed = parse_value(data, baseoffseti)
            baseoffseti += consumed
            if valuei is None:
                break
            value.append(valuei)
    elif data[0] == vt_space:
        value = None

        return value, 1
    elif data[0] == vt_float:
        value = struct.unpack(">d", data[3:3+value_len].rjust(8))[0]
    else:
        raise NotImplemented
    return value, 3+value_len


def serialize_value(value):
    if isinstance(value, int):
        # todo: more tight here
        return bytes([vt_integer])+struct.pack(">Hq", 8, value)
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
    elif isinstance(value, float):
        return bytes([vt_float])+struct.pack(">Hd", 8, value)
    else:
        raise TypeError("unsupported value:", value, "type:", type(value))
