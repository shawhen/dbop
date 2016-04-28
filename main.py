#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import sys
import json
import logbook
import argparse

from tornado import ioloop
from tornado import iostream
from tornado import tcpserver
from tornado import gen

from configs import db as m_configs_db
from configs import logs as m_configs_logs
from configs.codec import *

import drivers
from drivers import redis as m_d_redis
from drivers import mysql as m_d_mysql

logbook.set_datetime_format("local")
logbook.RotatingFileHandler(m_configs_logs.logpath,
                                level=m_configs_logs.loglevel,
                                backup_count=100,
                                max_size=1024 * 1024 * 10,
                                format_string='[{record.time}] {record.level_name} {record.filename} {record.lineno}: {record.message}'
                                ).push_application()

LOG = logbook.Logger("***Main***")

__id__ = 0
__registered_dbs__ = {
    # handle id: {
    #   dbname: db name
    #   tables: {
    #       tablename: table name
    #       columns: {
    #           colname: (bare/hash/list/set/sset, pos, int/text)
    #       }
    #   }
    # }
}

__stype_text2value__ = {
    "bare": col_stype_bare,
    "hash": col_stype_hash,
    "list": col_stype_list,
    "set": col_stype_set,
    "sset": col_stype_sset,
}

__vtype_text2value__ = {
    "int": vt_integer,
    "text": vt_text,
    "float": vt_float,
}


class DBOPServer(tcpserver.TCPServer):
    @gen.coroutine
    def handle_stream(self, stream, address):
        global __id__

        LOG.info("|-come a stream from: {0}".format(address))

        while True:
            # header(10): [0:3]-payload size, [4]-opcode
            try:
                header = yield stream.read_bytes(10)
                payload_size, op_code = struct.unpack(">LB", header[:5])
                payload = yield stream.read_bytes(payload_size)
            except iostream.StreamClosedError:
                break
            try:
                if op_code == opc_register_dbname:
                    # payload is the name(utf8 encoded)
                    dbname = payload.decode()
                    LOG.info("to register dbname({0}) with id({1})".format(dbname, __id__))
                    for dbname_handle, dbnamectx in __registered_dbs__.items():
                        if dbnamectx["dbname"] == dbname:
                            payload = ("0|"+str(dbname_handle)).encode()
                            break
                    else:
                        __registered_dbs__[__id__] = {"dbname": dbname, "tables": {}}
                        payload = ("0|"+str(__id__)).encode()
                        __id__ += 1
                elif op_code == opc_register_tablename:
                    # payload: dbname|tablename
                    dbname, tablename = payload.decode().split("|")
                    LOG.info("to register tablename({0})@dbname({1}) with id({2})".format(tablename, dbname, __id__))
                    for dbname_handle, dbnamectx in __registered_dbs__.items():
                        if dbnamectx["dbname"] == dbname:
                            dbtables = dbnamectx["tables"]
                            for dbtable_handle, dbtablectx in dbtables.items():
                                if dbtablectx["tablename"] == tablename:
                                    payload = ("0|"+str(dbtable_handle)).encode()
                                    break
                            else:
                                dbtables[__id__] = {"tablename": tablename, "columns": {}}
                                payload = ("0|"+str(__id__)).encode()
                                __id__ += 1
                            break
                    else:
                        payload = (str(errno_dbname_not_found)+"|"+dbname).encode()
                elif op_code == opc_register_columnname:
                    # payload: dbname|tablename|{colname: [pos, store type, value type]}
                    dbname, tablename, colstext = payload.decode().split("|")
                    LOG.info("to register columns({0})@tablename({1})@dbname({2})".format(colstext, tablename, dbname))
                    colsctx = json.loads(colstext)
                    for dbname_handle, dbnamectx in __registered_dbs__.items():
                        if dbnamectx["dbname"] == dbname:
                            dbtables = dbnamectx["tables"]
                            for tablename_handle, tablenamectx in dbtables.items():
                                if tablenamectx["tablename"] == tablename:
                                    columns = tablenamectx["columns"]
                                    for colname, colctx in colsctx.items():
                                        try:
                                            columns[colname] = (__stype_text2value__[colctx[1]], colctx[0], __vtype_text2value__[colctx[2]])
                                        except KeyError:
                                            payload = (str(errno_unsupported_data_type)+"|"+colctx[1]+"@"+str(colctx[0])).encode()

                                            raise ValueError("unsupported colctx:", colctx)

                                    payload = b"0|ok"
                                    break
                            else:
                                payload = (str(errno_tablename_not_found)+"|"+dbname+":"+tablename).encode()

                            break
                    else:
                        payload = (str(errno_dbname_not_found)+"|"+dbname).encode()
                else:
                    # dbname: [0]value type, [1] value len, value
                    # tablename: [0]value type, [1] value len, value
                    # op columns:
                    # columnname: [0]value type, [1] value len, value
                    # payload opcode
                    # columnvalue: [0]value type, [1] value len, value
                    # splitter: vt_space
                    # match columns: same with op columns(without opcode inside)
                    LOG.debug("|-to update:")

                    # dbname
                    baseoffset = 0
                    dbname, dbname_consumed = parse_value(payload, baseoffset)
                    baseoffset += dbname_consumed
                    if isinstance(dbname, int):  # db id
                        if dbname in __registered_dbs__:
                            dbnamectx = __registered_dbs__[dbname]
                            dbname = dbnamectx["dbname"]
                        else:
                            payload = (str(errno_dbname_not_found)+"|"+str(dbname)).encode()
                            raise ValueError("dbname not found:", dbname)
                    else:
                        for dbname_handle, dbnamectx in __registered_dbs__.items():
                            if dbnamectx["dbname"] == dbname:
                                break
                        else:
                            payload = (str(errno_dbname_not_found)+"|"+dbname).encode()
                            raise ValueError("dbname not found:", dbname)
                    LOG.debug("dbname: {0}".format(dbname))
                    # tablename
                    tablename, tablename_consumed = parse_value(payload, baseoffset)
                    baseoffset += tablename_consumed
                    if isinstance(tablename, int):  # table id
                        if tablename in dbnamectx["tables"]:
                            tablenamectx = dbnamectx["tables"][tablename]
                            tablename = tablenamectx["tablename"]
                        else:
                            payload = (str(errno_tablename_not_found)+"|"+dbname+":"+str(tablename)).encode()
                            raise ValueError("tablename not found:", tablename, "@dbname:", dbname)
                    else:
                        for tablename_handle, tablenamectx in dbnamectx.items():
                            if tablenamectx["tablename"] == tablename:
                                break
                        else:
                            payload = (str(errno_tablename_not_found)+"|"+dbname+":"+tablename).encode()
                            raise ValueError("tablename not found:", tablename, "@dbname:", dbname)
                    LOG.debug("tablename: {0}".format(tablename))
                    if op_code == opc_update:  # op columns
                        opcols = []  # (name, value, opcode)
                        oplockcols = []  # (name, opcode)
                        while True:
                            opcol_name, opcol_name_consumed = parse_value(payload, baseoffset)
                            baseoffset += opcol_name_consumed
                            if opcol_name is None:
                                break
                            if isinstance(opcol_name, int):  # column pos
                                colsctx = tablenamectx["columns"]
                                for colname, colctx in colsctx.items():
                                    if colctx[1] == opcol_name:
                                        opcol_name = colname
                                        break
                                else:
                                    payload = (str(errno_column_not_found)+"|"+dbname+":"+tablename+":"+str(opcol_name)).encode()
                                    raise ValueError("op column not found:", opcol_name, "@tablename:", tablename, "@dbname:", dbname)
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
                    elif op_code == opc_get:
                        getcols = []  # [asset, corporations...]
                        while True:
                            getcol_name, getcol_name_consumed = parse_value(payload, baseoffset)
                            baseoffset += getcol_name_consumed
                            if getcol_name is None:
                                break
                            if isinstance(getcol_name, int):  # column pos
                                colsctx = tablenamectx["columns"]
                                for colname, colctx in colsctx.items():
                                    if colctx[1] == getcol_name:
                                        getcol_name = colname
                                        break
                                else:
                                    payload = (str(errno_column_not_found)+"|"+dbname+":"+tablename+":"+str(getcol_name)).encode()
                                    raise ValueError("get column not found:", getcol_name, "@tablename:", tablename, "@dbname:", dbname)
                            getcols.append(getcol_name)
                        LOG.debug("getcols: {0}".format(getcols))
                    elif op_code == opc_delete:
                        delcols = []
                        while True:
                            delcol_name, delcol_name_consumed = parse_value(payload, baseoffset)
                            baseoffset += delcol_name_consumed
                            if delcol_name is None:
                                break
                            if isinstance(delcol_name, int):  # column pos
                                colsctx = tablenamectx["columns"]
                                for colname, colctx in colsctx.items():
                                    if colctx[1] == delcol_name:
                                        delcol_name = colname
                                        break
                                else:
                                    payload = (str(errno_column_not_found)+"|"+dbname+":"+tablename+":"+str(delcol_name)).encode()
                                    raise ValueError("get column not found:", delcol_name, "@tablename:", tablename, "@dbname:", dbname)
                            delcols.append(delcol_name)
                        LOG.debug("|-del cols: {0}".format(delcols))
                    else:
                        raise NotImplementedError()
                    matchcols = []  # (name, value, match_type, match_relation)
                    # match columns
                    while True:
                        matchcol_name, matchcol_name_consumed = parse_value(payload, baseoffset)
                        baseoffset += matchcol_name_consumed
                        if matchcol_name is None:
                            break
                        if isinstance(matchcol_name, int):
                            colsctx = tablenamectx["columns"]
                            for colname, colctx in colsctx.items():
                                if colctx[1] == matchcol_name:
                                    matchcol_name = colname
                                    break
                            else:
                                payload = (str(errno_column_not_found)+"|"+dbname+":"+tablename+":"+str(matchcol_name)).encode()
                                raise ValueError("match column not found:", matchcol_name, "@tablename:", tablename, "@dbname:", dbname)
                        match_type = payload[baseoffset]
                        baseoffset += 1
                        matchcol_value, matchcol_value_consumed = parse_value(payload, baseoffset)
                        baseoffset += matchcol_value_consumed
                        match_relation = payload[baseoffset]
                        baseoffset += 1
                        matchcols.append((matchcol_name, matchcol_value, match_type, match_relation))
                    LOG.debug("|-matchcols: {0}".format(matchcols))

                    if op_code == opc_update:
                        if m_configs_db.driver == drivers.driver_redis:
                            payload = yield m_d_redis.update(dbname, tablename, opcols, oplockcols, matchcols, tablenamectx["columns"])
                        elif m_configs_db.driver == drivers.driver_mysql:
                            payload = yield m_d_mysql.update(dbname, tablename, opcols, oplockcols, matchcols, tablenamectx["columns"])
                        else:
                            raise NotImplementedError()
                    elif op_code == opc_get:
                        if m_configs_db.driver == drivers.driver_redis:
                            payload = yield m_d_redis.get(dbname, tablename, getcols, matchcols, tablenamectx["columns"])
                        else:
                            raise NotImplementedError()
                    elif op_code == opc_delete:
                        if m_configs_db.driver == drivers.driver_redis:
                            payload = yield m_d_redis.delete(dbname, tablename, delcols, matchcols, tablenamectx["columns"])
                        else:
                            raise NotImplementedError()
                    payload = b"0|"+json.dumps(payload).encode()
            except Exception as e:
                import traceback
                traceback.print_exc()
                LOG.exception(traceback.format_exc())
                payload = b"102|"+json.dumps(traceback.format_exc()).encode()
            finally:
                header = (struct.pack(">LB", len(payload), op_code)).ljust(10)
                try:
                    LOG.debug("|-payload: {0}".format(payload))
                    yield stream.write(header+payload)
                except iostream.StreamClosedError:
                    pass


def boot(listen_host, listen_port, node, db_host, db_port):
    """

    :param listen_port:
    :param node: redis/mysql
    :param db_host:
    :param db_port:
    :param kwargs:
    :return:
    """
    if node == "redis":
        m_configs_db.driver = drivers.driver_redis
    elif node == "mysql":
        m_configs_db.driver = drivers.driver_mysql
    else:
        raise ValueError("unsupported node type:", node)
    m_configs_db.host = db_host
    m_configs_db.port = db_port
    m_configs_db.init()

    io_loop = ioloop.IOLoop.current()
    dbopserver = DBOPServer()
    dbopserver.listen(listen_port, address=listen_host)
    io_loop.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen_host", help="the listen host")
    parser.add_argument("--listen_port", help="the listen port")
    parser.add_argument("--node", help="redis op or mysql op")
    parser.add_argument("--dbhost", help="the real database's host")
    parser.add_argument("--dbport", help="the real database's port")
    args = parser.parse_args()

    boot(args.listen_host, args.listen_port, args.node, args.dbhost, args.dbport)
