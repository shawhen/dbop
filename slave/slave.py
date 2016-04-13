#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import os
import errno
import struct
import logbook
import argparse
import functools

import gevent
from gevent import socket as m_socket

from protocol import *


logbook.set_datetime_format("local")
LOG = logbook.Logger("***Slave***")


def slave(internal_host, internal_port, worker_host, worker_port, ptype, iid, *args, **kwargs):
    print("|-internal host:", internal_host, "internal port:", internal_port,
          "worker host:", worker_host, "worker port:", worker_port, "port type:", ptype, "iid:", iid,
          "args:", args, "kwargs:", kwargs)
    try:
        os.makedirs("logs")
    except OSError as e:
        if e.errno == 17:  # file exist
            pass
        else:
            raise
    with logbook.RotatingFileHandler("logs/slave.log".format(internal_port),
                                    level=logbook.DEBUG,
                                    backup_count=100,
                                    max_size=1024 * 1024 * 10,
                                    format_string='[{record.time}] {record.level_name} {record.filename} {record.lineno}: {record.message}'
                                    ).applicationbound():
        g = gevent.spawn(slave_co, internal_host, internal_port, worker_host, worker_port, ptype, iid)
        g.join()
        LOG.debug("|-slave finished")


def shadow_co(peer, lpeer, peerid, shared_ctx, slavelet):
    clet = gevent.getcurrent()

    LOG.debug("|-serve a peer<{0}>(id: {1})".format(peer, peerid))

    while True:
        try:
            dp = peer.recv(40960)
            if len(dp) == 0:
                __shadow_close_cb__(peer, peerid, lpeer, shared_ctx, slavelet)
                break
        except m_socket.error as e:
            if e.errno == errno.EBADF:  # shadow closed
                __shadow_close_cb__(peer, peerid, lpeer, shared_ctx, slavelet)
                break
            else:
                raise

        lheader_dp = struct.pack(pkg_header_format, len(dp), peerid, b"DP")
        lpeer.sendall(lheader_dp+dp)


def __shadow_close_cb__(peer, peerid, lpeer, shared_ctx, slavelet):
    LOG.debug("|-shadow closed, peer: {0}, peerid: {1}".format(peer, peerid))
    if peerid in shared_ctx["eshadowpeers"]:
        del shared_ctx["eshadowpeers"][peerid]

        lheader_lc = struct.pack(pkg_header_format, 0, peerid, b"LC")
        try:
            lpeer.sendall(lheader_lc)
        except m_socket.error as e:
            if e.errno == errno.EBADF:
                slavelet.kill(e)
            else:
                raise


def __slave_close_cb__(shared_ctx, let=None):
    LOG.debug("|-slave closed, shared_ctx: {0}, exception: {1}".format(shared_ctx, "" if let is None else let.exception))
    for peerid, peer in shared_ctx["eshadowpeers"].items():
        peer.close()


def slave_co(ihost, iport, whost, wport, ptype, iid, *args, **kwargs):
    def keep_live_deadline_cb(let):
        LOG.debug("|-slave sock has reached deadline")
        let.kill()

    clet = gevent.getcurrent()

    lpeer = m_socket.socket()
    lpeer.connect((ihost, iport))
    print("connected to ", ihost, iport)
    # report version
    protocol_version_b = (str(protocol_version[0])+"."+str(protocol_version[1])+"."+str(protocol_version[2])).encode()
    lheader_rv = struct.pack(pkg_header_format, len(protocol_version_b), peerid4s, b"RV")
    lpeer.sendall(lheader_rv+protocol_version_b)
    lheader_rv = recvexactly(lpeer, pkg_header_size)
    payload_size_rv, _, rv_cmd = struct.unpack(pkg_header_format, lheader_rv)
    lpayload_rv = recvexactly(lpeer, payload_size_rv)
    if rv_cmd == b"VA":  # version accepted
        pass
    elif rv_cmd == b"VR":  # version rejected
        LOG.debug("|-adapter's version({0}) had been rejected, listener's version is: {1}".format(protocol_version_b, lpayload_rv))
        raise ValueError("adapter version rejected:", protocol_version_b, "listener's version:", lpayload_rv)
    elif rv_cmd == b"VC":  # version compatible
        LOG.debug("|-adapter's version({0}) compatible, listener's version is: {1}".format(protocol_version_b, lpayload_rv))
    else:
        raise ValueError("|-unsupported report version cmd:", rv_cmd)
    # report id
    iid_b = iid.encode()
    lheader_iid = struct.pack(pkg_header_format, len(iid_b), 0, b"ID")
    lpeer.sendall(lheader_iid+iid_b)
    lheader_iid = recvexactly(lpeer, pkg_header_size)
    iid_payload_size, _, iid_cmd = struct.unpack(pkg_header_format, lheader_iid)
    if iid_cmd == b"IA":  # identity accepted
        lheader_eo = struct.pack(pkg_header_format, 0, 0, b"EO")
        lpeer.sendall(lheader_eo)
    elif iid_cmd == b"ID":  # identity duplicated
        raise ValueError("ID(iid: {0}) duplicated".format(iid))
    else:  # identity error
        raise ValueError("ID(iid: {0}) response failed:".format(iid), lheader_iid)
    print("everything is ok")

    shared_ctx = {"eshadowpeers": {}}
    clet.link_exception(functools.partial(__slave_close_cb__, shared_ctx))
    kl_deadline = None
    try:
        while True:
            lheader = recvexactly(lpeer, pkg_header_size)
            lpayload_len, epeerid, lcmd = struct.unpack(pkg_header_format, lheader)
            lpayload = recvexactly(lpeer, lpayload_len)
            print("|-listener header:", lheader, "listener payload len:", len(lpayload))
            if lcmd == b"NC":  # new connection
                LOG.debug("|-new connection come, id: {0}".format(epeerid))
                try:
                    epeer = m_socket.socket()
                    epeer.connect((whost, wport))
                    shared_ctx["eshadowpeers"][epeerid] = epeer
                    gevent.spawn(shadow_co, epeer, lpeer, epeerid, shared_ctx, clet)
                    LOG.debug("|made a connection to worker({0}:{1})".format(whost, wport))
                except m_socket.error as e:
                    LOG.debug("|-connect worker failed, connection(id: {0}) lost".format(epeerid))

                    import traceback
                    traceback.print_exc()
                    LOG.exception(traceback.format_exc())

                    LOG.debug("|-send LC({0}) to listener({1})".format(epeerid, lpeer))
                    lheader_lc = struct.pack(pkg_header_format, 0, epeerid, b"LC")
                    lpeer.sendall(lheader_lc)
            elif lcmd == b"KL":  # keep-live
                kl_deadline_sec = int(lpayload)
                if kl_deadline is not None:
                    kl_deadline.cancel()
                kl_deadline = gevent.Timeout(seconds=kl_deadline_sec)
                kl_deadline.start()

                lpeer.sendall(lheader+lpayload)
            elif lcmd == b"DP":  # data payload
                eshadowpeer = shared_ctx["eshadowpeers"].get(epeerid, None)
                if eshadowpeer is not None:
                    try:
                        eshadowpeer.sendall(lpayload)
                    except m_socket.error:
                        __shadow_close_cb__(eshadowpeer, epeerid, lpeer, shared_ctx, clet)
                    print("|-written payload(len: {0}) to worker({1})".format(len(lpayload), eshadowpeer))
            elif lcmd == b"LC":  # lost connection
                LOG.debug("|-lost connection, id: {0}".format(epeerid))
                eshadowpeer = shared_ctx["eshadowpeers"].get(epeerid, None)
                if eshadowpeer is not None:
                    eshadowpeer.close()
            else:
                raise ValueError("unsupported command:", lheader, lpayload)
    except gevent.Timeout as e:
        if e == kl_deadline:
            keep_live_deadline_cb(clet)
        else:
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", help="the listener's host(ip or domain)")
    parser.add_argument("--port", help="the listener's port")
    parser.add_argument("--whost", help="the worker's host(ip or domain)")
    parser.add_argument("--wport", help="the worker's port")
    parser.add_argument("--type", default="red", help="the listener's type")
    parser.add_argument("--iid", help="the slave's id")
    args = parser.parse_args()

    slave(args.host, args.port, args.whost, args.wport, args.type, args.iid)
