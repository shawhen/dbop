#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import logbook

from tornado import gen

from configs import db as m_configs_db
from configs.codec import *

LOG = logbook.Logger("***Driver Redis***")


def make_matchkey(matchcols):
    matchkey = b""  # e.g. 10=uid& 1=account_type&1=from&
    for matchcol_name, matchcol_value, matchcol_type, matchcol_relation in matchcols:
        if matchcol_type != match_equal or matchcol_relation != match_and:
            raise ValueError("redis update matchcols can only be: = and")
        else:
            matchkey += str(matchcol_value).encode()+b"="+matchcol_name.encode()+b"&"
    LOG.debug("|-redis match key: {0}".format(matchkey))
    return matchkey


def make_hmsetkey(dbname, tname, matchkey):
    hmsetkey = matchkey+b"@"+tname.encode()+b":"+dbname.encode()  # 10=uid&@users:wealthempire
    return hmsetkey


def make_orphankey(dbname, tname, colname, matchkey):
    hmsetkey = make_hmsetkey(dbname, tname, matchkey)
    orphankey = hmsetkey+b"-"+colname.encode()
    return orphankey


@gen.coroutine
def update(dbname, tname, opcols, oplockcols, matchcols, cols):
    """

    :param dbname:
    :param tname:
    :param opcols:
    :param oplockcols:
    :param matchcols:
    :return: {pos: value}
    """
    LOG.debug("|-update {0}:{1}, opcols: {2}, oplockcols: {3}, matchcols: {4}"
              .format(dbname, tname, opcols, oplockcols, matchcols))
    matchkey = make_matchkey(matchcols)  # e.g. 10=uid& 1=account_type&1=from&
    fullkey4hmset = make_hmsetkey(dbname, tname, matchkey) # 10=uid&@users:wealthempire
    LOG.debug("|-redis full key for hmset: {0}".format(fullkey4hmset))
    hmsetkv = {}
    msetkv = {}
    ret = {}  # pos: value
    for opcol_name, opcol_value, opcol_opcode in opcols:
        opcolctx = cols[opcol_name]  # store type, pos, data type

        if opcolctx[0] == col_stype_bare:
            if opcol_opcode == popc_assign:  # =
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                msetkv[key] = opcol_value
            elif opcol_opcode == popc_add:  # +
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-incrby: key({0}) value({1})".format(key, opcol_value))
                ret[opcolctx[1]] = m_configs_db.red.incrby(key, opcol_value)
            elif opcol_opcode == popc_sub:  # -
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-incrby: key({0}) value({1})".format(key, -opcol_value))
                ret[opcolctx[1]] = m_configs_db.red.incrby(key, -opcol_value)
            else:
                raise NotImplemented
        elif opcolctx[0] == col_stype_hash:
            if opcol_opcode == popc_assign:  # =
                hmsetkv[opcol_name] = opcol_value
            elif opcol_opcode == popc_add:  # +
                LOG.debug("|-hincrby: key({0}) field({1}) value({2})".format(fullkey4hmset, opcol_name, opcol_value))
                if opcolctx[2] == vt_integer:
                    ret[opcolctx[1]] = m_configs_db.red.hincrby(fullkey4hmset, opcol_name, opcol_value)
                elif opcolctx[2] == vt_float:
                    ret[opcolctx[1]] = m_configs_db.red.hincrbyfloat(fullkey4hmset, opcol_name, opcol_value)
                else:
                    raise ValueError("unsupported + for data type, opcolctx:", opcolctx)
            elif opcol_opcode == popc_sub:  # -
                LOG.debug("|-hincrby: key({0}) field({1}) value({2})".format(fullkey4hmset, opcol_name, -opcol_value))
                if opcolctx[2] == vt_integer:
                    ret[opcolctx[1]] = m_configs_db.red.hincrby(fullkey4hmset, opcol_name, -opcol_value)
                elif opcolctx[2] == vt_float:
                    ret[opcolctx[1]] = m_configs_db.red.hincrbyfloat(fullkey4hmset, opcol_name, -opcol_value)
                else:
                    raise ValueError("unsupported - for data type, opcolctx:", opcolctx)
            else:
                raise NotImplemented
        elif opcolctx[0] == col_stype_list:
            if opcol_opcode == popc_extend:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-rpush key({0}) values({1})".format(key, opcol_value))
                ret[opcolctx[1]] = m_configs_db.red.rpush(key, *opcol_value)
            elif opcol_opcode == popc_remove:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-lrem key({0}) value({1}) count(1)".format(key, opcol_value))
                for opcol_valuei in opcol_value:
                    ret[opcolctx[1]] = m_configs_db.red.lrem(key, 0, opcol_valuei)
            elif opcol_opcode == popc_assign:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-list assign key({0}) values({1})".format(key, opcol_value))
                m_configs_db.red.delete(key)
                if len(opcol_value) != 0:
                    m_configs_db.red.rpush(key, *opcol_value)
            else:
                raise NotImplemented
        elif opcolctx[0] == col_stype_set:
            if opcol_opcode == popc_sadd or opcol_opcode == popc_extend:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-sadd key({0}) values({1})".format(key, opcol_value))
                if len(opcol_value) != 0:
                    ret[opcolctx[1]] = m_configs_db.red.sadd(key, *opcol_value)
            elif opcol_opcode == popc_sremove or opcol_opcode == popc_remove:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-srem key({0}) values({1})".format(key, opcol_value))
                ret[opcolctx[1]] = m_configs_db.red.srem(key, *opcol_value)
            elif opcol_opcode == popc_assign:
                key = make_orphankey(dbname, tname, opcol_name, matchkey)
                LOG.debug("|-set assign key({0}) values({1})".format(key, opcol_value))
                m_configs_db.red.delete(key)
                if len(opcol_value) != 0:
                    ret[opcolctx[1]] = m_configs_db.red.sadd(key, *opcol_value)
            else:
                raise NotImplemented
        else:
            raise ValueError("unsupported opcolctx:", opcolctx)
    LOG.debug("|-hmsetkv: {0}".format(hmsetkv))
    LOG.debug("|-msetkv: {0}".format(msetkv))
    if len(hmsetkv) != 0:
        ret["#".join(map(lambda keyname: str(cols[keyname][1]), hmsetkv.keys()))] = m_configs_db.red.hmset(fullkey4hmset, hmsetkv)
    if len(msetkv) != 0:
        ret["&".join(map(lambda keyname: str(cols[keyname][1]), msetkv.keys()))] = m_configs_db.red.mset(**msetkv)
    LOG.debug("|-ret: {0}".format(ret))

    return ret


@gen.coroutine
def delete(dbname, tname, delcols, matchcols, cols):
    if len(delcols) == 0:
        delcols = tuple(cols.keys())
    matchkey = make_matchkey(matchcols)
    hmsetkey = make_hmsetkey(dbname, tname, matchkey)  # 10=uid&@users:wealthempire
    orphankeys = []
    for delcol in delcols:
        delcolctx = cols[delcol]
        if delcolctx[0] == col_stype_bare or delcolctx[0] == col_stype_list or\
                        delcolctx[0] == col_stype_set or delcolctx[0] == col_stype_sset:
            orphankey = make_orphankey(dbname, tname, delcol, matchkey)
            orphankeys.append(orphankey)
    return m_configs_db.red.delete(hmsetkey, *orphankeys)


@gen.coroutine
def get(dbname, tname, getcols, matchcols, cols):
    """

    :param dbname:
    :param tname:
    :param getcols:
    :param matchcols:
    :param cols: [(col_stype, pos, value_type)]
    :return: {pos: value}
    """
    if len(getcols) == 0:
        getcols = tuple(cols.keys())

    matchkey = make_matchkey(matchcols)
    hmgetkey = make_hmsetkey(dbname, tname, matchkey)

    hmgetfields = []  # hash fields
    barekeykw = {}  # bare keys-col name
    listkeykw = {}  # list keys-col name
    setkeykw = {}  # set keys-col name
    ssetkeykw = {}  # sorted set keys-col name
    for colname in getcols:
        colctx = cols[colname]
        if colctx[0] == col_stype_hash:
            hmgetfields.append(colname)
        else:
            orphankey = make_orphankey(dbname, tname, colname, matchkey)
            if colctx[0] == col_stype_bare:
                barekeykw[orphankey] = colname
            elif colctx[0] == col_stype_list:
                listkeykw[orphankey] = colname
            elif colctx[0] == col_stype_set:
                setkeykw[orphankey] = colname
            elif colctx[0] == col_stype_sset:
                ssetkeykw[orphankey] = colname
    LOG.debug("|-get hmgetfileds: {0}, barekeykw: {1}, listkeykw: {2}, setkeykw: {3}, ssetkeykw: {4}"
              .format(hmgetfields, barekeykw, listkeykw, setkeykw, ssetkeykw))
    colpv = {}  # col pos: col value
    if len(hmgetfields) != 0:
        hmgetvalues = m_configs_db.red.hmget(hmgetkey, hmgetfields)
        for hmgetfield, hmgetvalue in zip(hmgetfields, hmgetvalues):
            colctx = cols[hmgetfield]
            if colctx[2] == vt_integer:
                colpv[colctx[1]] = int(hmgetvalue) if hmgetvalue is not None else 0
            elif colctx[2] == vt_float:
                colpv[colctx[1]] = float(hmgetvalue) if hmgetvalue is not None else 0.0
            elif colctx[2] == vt_text:
                colpv[colctx[1]] = hmgetvalue.decode() if hmgetvalue is not None else ""
            else:
                raise ValueError("unsupported col ctx:", colctx, "hmgetfield:", hmgetfield, " hmgetvalue:", hmgetvalue)
    if len(barekeykw) != 0:
        barekeys = barekeykw.keys()
        barevalues = m_configs_db.red.mget(barekeys)
        for barekey, barevalue in zip(barekeys, barevalues):
            colctx = cols[barekeykw[barekey]]
            if colctx[2] == vt_integer:
                colpv[colctx[1]] = int(barevalue) if barevalue is not None else 0
            elif colctx[2] == vt_float:
                colpv[colctx[1]] = float(barevalue) if barevalue is not None else 0.0
            elif colctx[2] == vt_text:
                colpv[colctx[1]] = barevalue.decode() if barevalue is not None else ""
            else:
                raise ValueError("unsupported col ctx:", colctx, "bare key:", barekey, "bare value:", barevalue)
    for listkeyk, listkeycol in listkeykw.items():
        colctx = cols[listkeycol]
        listkeyvalues = m_configs_db.red.lrange(listkeyk, 0, -1)
        if colctx[2] == vt_integer:
            listkeyvalues = tuple(map(lambda lv: int(lv), listkeyvalues))
        elif colctx[2] == vt_float:
            listkeyvalues = tuple(map(lambda lv: float(lv), listkeyvalues))
        elif colctx[2] == vt_text:
            listkeyvalues = tuple(map(lambda lv: lv.decode(), listkeyvalues))
        else:
            raise ValueError("unsupported col ctx:", colctx, "list key:", listkeyk, "list value:", listkeyvalues)
        colpv[colctx[1]] = listkeyvalues
    for setkeyk, setkeycol in setkeykw.items():
        colctx = cols[setkeycol]
        setkeyvalues = m_configs_db.red.smembers(setkeyk)
        if colctx[2] == vt_integer:
            setkeyvalues = tuple(map(lambda sv: int(sv), setkeyvalues))
        elif colctx[2] == vt_float:
            setkeyvalues = tuple(map(lambda sv: float(sv), setkeyvalues))
        elif colctx[2] == vt_text:
            setkeyvalues = tuple(map(lambda sv: sv.decode(), setkeyvalues))
        else:
            raise ValueError("unsupported col ctx:", colctx, "set key:", setkeyk, "set value:", setkeyvalues)
        colpv[colctx[1]] = list(setkeyvalues)
    for ssetkeyk, ssetkeycol in ssetkeykw.items():
        colctx = cols[ssetkeycol]
        ssetkeyvalues = m_configs_db.red.zrange(ssetkeyk, 0, -1)
        if colctx[2] == vt_integer:
            ssetkeyvalues = tuple(map(lambda ssv: int(ssv), ssetkeyvalues))
        elif colctx[2] == vt_float:
            ssetkeyvalues = tuple(map(lambda ssv: float(ssv), ssetkeyvalues))
        elif colctx[2] == vt_text:
            ssetkeyvalues = tuple(map(lambda ssv: ssv.decode(), ssetkeyvalues))
        else:
            raise ValueError("unsupported col ctx:", colctx, "sset key:", ssetkeyk, "sset value:", ssetkeyvalues)
        colpv[colctx[1]] = list(ssetkeyvalues)

    return colpv
