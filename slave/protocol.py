#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

protocol_version = (2, 0, 0)  # major minor fix

pkg_header_size = 10
pkg_header_format = ">LL2s"  # payload-len peer-id command

keeplive_gap = 180

peerid4s = 1  # slave report version
peerid4a = 2  # adapter report version


def recvexactly(sock, size):
    recved = b""
    while True:
        recved_once = sock.recv(size - len(recved))
        if len(recved_once) > 0:
            recved += recved_once
            if len(recved) == size:
                break
        else:
            break

    assert len(recved) == size
    return recved
