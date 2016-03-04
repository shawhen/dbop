#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import logbook

from tornado import gen
from tornado import locks

from configs import db as m_configs_db
from configs.codec import *

LOG = logbook.Logger("***Driver MySQL***")


__col_locks__ = {
    # dbname: {
    #       tablename: {
    #           columnone: {
    #
    #           }
    #       }
    # }
}


@gen.coroutine
def update(dbname, tname, opcols, oplockcols, matchcols):
    LOG.debug("|-update {0}:{1}, opcols: {2}, oplockcols: {3}, matchcols: {4}"
              .format(dbname, tname, opcols, oplockcols, matchcols))
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
    tablelocks = __col_locks__[dbname][tname]
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
        sql += " FROM `{0}`.`{1}` ".format(dbname, tname)
        sql += " "+match_clause+" ;"
        # get out lock columns
        LOG.debug("|-execute sql: {0}".format(sql))
        try:
            cursor = yield m_configs_db.pool.execute(sql)
        except Exception as e:
            import traceback
            traceback.print_exc()

            LOG.error(traceback.format_exc())
        LOG.debug("|-executed")
        oplockcols_db_values = list(cursor.fetchone())
    sql = '''UPDATE `{db}`.`{table}` SET '''.format(db=dbname, table=tname)
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
        yield m_configs_db.pool.execute(sql, update_params)
    except Exception as e:
        import traceback
        traceback.print_exc()

        LOG.error(traceback.format_exc())
    LOG.debug("|-executed")
    # release lock columns
    for opcollock in opcollocks:
        opcollock.set()