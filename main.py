#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import struct
import logbook

from tornado import ioloop
from tornado import iostream
from tornado import tcpserver
from tornado import gen
from tornado import locks

from configs import db
from configs import logs as m_configs_logs

logbook.set_datetime_format("local")
logbook.RotatingFileHandler(m_configs_logs.logpath,
                                level=m_configs_logs.loglevel,
                                backup_count=100,
                                max_size=1024 * 1024 * 10,
                                format_string='[{record.time}] {record.level_name} {record.filename} {record.lineno}: {record.message}'
                                ).push_application()

LOG = logbook.Logger("***Main***")


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
popc_assign = 10

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


col_locks = {
    # dbname: {
    #       tablename: {
    #           columnone: {
    #
    #           }
    #       }
    # }
}


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
    else:
        raise NotImplemented


class DBOPServer(tcpserver.TCPServer):
    @gen.coroutine
    def handle_stream(self, stream, address):
        global col_locks

        LOG.info("|-come a stream from: {0}".format(address))

        while True:
            # header(10): [0:3]-payload size, [4]-opcode
            try:
                header = yield stream.read_bytes(10)
                payload_size, op_code = struct.unpack(">LB", header[:5])
                payload = yield stream.read_bytes(payload_size)
            except iostream.StreamClosedError:
                break
            if op_code == opc_payload:
                # dbname: [0]value type, [1] value len, value
                # tablename: [0]value type, [1] value len, value
                # op columns:
                # columnname: [0]value type, [1] value len, value
                # payload opcode
                # columnvalue: [0]value type, [1] value len, value
                # splitter: vt_space
                # match columns: same with op columns(without opcode inside)

                # dbname
                baseoffset = 0
                dbname, dbname_consumed = parse_value(payload, baseoffset)
                baseoffset += dbname_consumed
                if isinstance(dbname, int):
                    raise NotImplemented
                if dbname not in col_locks:
                    col_locks[dbname] = {}
                LOG.debug("dbname: {0}".format(dbname))
                # tablename
                tablename, tablename_consumed = parse_value(payload, baseoffset)
                baseoffset += tablename_consumed
                if isinstance(tablename, int):
                    raise NotImplemented
                LOG.debug("tablename: {0}".format(tablename))
                if tablename not in col_locks[dbname]:
                    col_locks[dbname][tablename] = {}
                tablelocks = col_locks[dbname][tablename]
                # op columns
                opcols = []  # (name, value, opcode)
                oplockcols = []  # (name, opcode)
                while True:
                    opcol_name, opcol_name_consumed = parse_value(payload, baseoffset)
                    baseoffset += opcol_name_consumed
                    if opcol_name is None:
                        break
                    if isinstance(opcol_name, int):
                        raise NotImplemented
                    payload_opcode = payload[baseoffset]
                    baseoffset += 1
                    # list&string operation must be locked
                    if payload_opcode == popc_extend or payload_opcode == popc_remove or\
                            payload_opcode == popc_append or payload_opcode == popc_prepend or payload_opcode == popc_replace:  # string
                        # we need lock this column
                        oplockcols.append((opcol_name, payload_opcode))

                    opcol_value, opcol_value_consumed = parse_value(payload, baseoffset)
                    baseoffset += opcol_value_consumed
                    opcols.append((opcol_name, opcol_value, payload_opcode))
                LOG.debug("opcols: {0}".format(opcols))
                LOG.debug("oplockcols: {0}".format(oplockcols))
                matchcols = []  # (name, value, match_type, match_relation)
                # match columns
                while True:
                    matchcol_name, matchcol_name_consumed = parse_value(payload, baseoffset)
                    baseoffset += matchcol_name_consumed
                    if matchcol_name is None:
                        break
                    if isinstance(matchcol_name, int):
                        raise NotImplemented
                    match_type = payload[baseoffset]
                    baseoffset += 1
                    matchcol_value, matchcol_value_consumed = parse_value(payload, baseoffset)
                    baseoffset += matchcol_value_consumed
                    match_relation = payload[baseoffset]
                    baseoffset += 1
                    matchcols.append((matchcol_name, matchcol_value, match_type, match_relation))
                LOG.debug("matchcols: {0}".format(matchcols))
                # # match clause
                match_clause = ""
                if len(matchcols) != 0:
                    match_clause = ''' WHERE '''
                for matchcol_num in range(0, len(matchcols)):
                    matchcol = matchcols[matchcol_num]
                    if isinstance(matchcol[1], str):
                        match_clausei = ''' `{0}`{1}"{2}" '''\
                            .format(matchcol[0], "=" if matchcol[2] == match_equal else "!=", matchcol[1])
                    else:
                        match_typei = "="
                        if matchcol[2] == match_equal:
                            match_typei = "="
                        elif matchcol[2] == match_not_equal:
                            match_typei = "!="
                        elif matchcol[2] == match_greater:
                            match_typei = ">"
                        elif matchcol[2] == match_greater_equal:
                            match_typei = ">="
                        elif matchcol[2] == match_lesser:
                            match_typei = "<"
                        elif matchcol[2] == match_lesser_equal:
                            match_typei = "<="
                        else:
                            raise NotImplemented
                        match_clausei = ''' `{0}`{1}{2} '''\
                            .format(matchcol[0], match_typei, matchcol[1])
                    if matchcol_num != 0:  # consider match relation
                        if matchcol[3] == match_and:
                            match_clause += " AND "+match_clausei
                        elif matchcol[3] == match_or:
                            match_clause += " OR "+match_clausei
                        else:
                            raise NotImplemented
                    else:
                        match_clause += match_clausei
                opcollocks = []
                if len(oplockcols) != 0:
                    sql = '''SELECT '''+",".join(map(lambda col: "`"+str(col[0])+"`", oplockcols))
                    # acquire lock columns
                    # todo: more precise
                    for oplockcol in oplockcols:
                        if oplockcol[0] not in tablelocks:
                            lock = locks.Event()
                            opcollocks.append(lock)
                            lock.clear()
                            tablelocks[oplockcol[0]] = lock
                        else:
                            lock = tablelocks[oplockcol[0]]
                            opcollocks.append(lock)
                            yield lock.wait()
                            lock.clear()
                    sql += " FROM `{0}`.`{1}` ".format(dbname, tablename)
                    sql += " "+match_clause+" ;"
                    # get out lock columns
                    LOG.debug("|-execute sql: {0}".format(sql))
                    try:
                        cursor = yield db.pool.execute(sql)
                    except Exception as e:
                        import traceback
                        traceback.print_exc()

                        LOG.error(traceback.format_exc())
                    LOG.debug("|-executed")
                    oplockcols_db_values = list(cursor.fetchone())
                sql = '''UPDATE `{db}`.`{table}` SET '''.format(db=dbname, table=tablename)
                update_params = []
                update_opcols = []
                for col_name, col_value, op_code in opcols:
                    if op_code == popc_add:  # +
                        update_opcols.append(''' `{name}`=`{name}`+{delta} '''.format(name=col_name, delta=col_value))
                    elif op_code == popc_sub:  # -
                        update_opcols.append(''' `{name}`=`{name}`-{delta} '''.format(name=col_name, delta=col_value))
                    elif op_code == popc_multi:  # *
                        update_opcols.append(''' `{name}`=`{name}`*{delta} '''.format(name=col_name, delta=col_value))
                    elif op_code == popc_divide:  # /
                        update_opcols.append(''' `{name}`=`{name}`/{delta} '''.format(name=col_name, delta=col_value))
                    elif op_code == popc_mod:  # %
                        update_opcols.append(''' `{name}`=`{name}`%{delta} '''.format(name=col_name, delta=col_value))
                    elif op_code == popc_assign:  # =
                        update_opcols.append(''' `{name}`=%s '''.format(name=col_name))
                        update_params.append(col_value)
                    elif op_code == popc_extend:  # extend list
                        col_db_value = oplockcols_db_values.pop(0)
                        if col_db_value is not None:
                            col_db_list, _ = parse_value(col_db_value)
                            if col_db_list is None:
                                col_db_list = []
                        else:
                            col_db_list = []
                        col_db_list.extend(col_value)
                        col_db_list_bytes = serialize_value(col_db_list)
                        update_opcols.append(''' `{name}`=%s '''.format(name=col_name))
                        update_params.append(col_db_list_bytes)
                    elif op_code == popc_remove:  # remove from list
                        col_db_value = oplockcols_db_values.pop(0)
                        if col_db_value is not None:
                            col_db_list, _ = parse_value(col_db_value)
                            if col_db_list is None:
                                col_db_list = []
                        else:
                            col_db_list = []
                        for col_valuei in col_value:
                            if col_valuei in col_db_list:
                                col_db_list.remove(col_valuei)
                        col_db_list_bytes = serialize_value(col_db_list)
                        update_opcols.append(''' `{name}`=%s '''.format(name=col_name))
                        update_params.append(col_db_list_bytes)
                    elif op_code == popc_append:  # string append
                        col_db_value = oplockcols_db_values.pop(0)
                        col_db_value = col_db_value if col_db_value is not None else ""
                        update_opcols.append(''' `{name}`="{value}" '''.format(name=col_name, value=col_db_value+col_value))
                    elif op_code == popc_prepend:  # string prepend
                        col_db_value = oplockcols_db_values.pop(0)
                        col_db_value = col_db_value if col_db_value is not None else ""
                        update_opcols.append(''' `{name}`="{value}" '''.format(name=col_name, value=col_value+col_db_value))
                    elif op_code == popc_replace:  # string replace
                        col_db_value = oplockcols_db_values.pop(0)
                        update_opcols.append(''' `{name}`="{value}" '''.format(name=col_name, value=col_db_value.replace(col_value[0], col_value[1])))
                    else:
                        raise NotImplemented
                sql += ",".join(update_opcols)
                sql += " "+match_clause+" ;"
                LOG.debug("|-execute sql: {0}, params: {1}".format(sql, update_params))
                try:
                    yield db.pool.execute(sql, update_params)
                except Exception as e:
                    import traceback
                    traceback.print_exc()

                    LOG.error(traceback.format_exc())
                LOG.debug("|-executed")
                # release lock columns
                for opcollock in opcollocks:
                    opcollock.set()
            else:
                raise NotImplemented

if __name__ == "__main__":
    @gen.coroutine
    def test():
        yield db.pool.execute("UPDATE `wealthempire_develop`.`users` SET `corporations`=%s", b'x03x00x0bx00x00x08x00x00x00x00x00x00x00x02')

    io_loop = ioloop.IOLoop.current()
    dbopserver = DBOPServer()
    dbopserver.listen(32000)
    io_loop.start()
