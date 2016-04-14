#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import os
import logbook

try:
    os.makedirs("logs")
except FileExistsError:
    pass
logpath = "logs/log.log"
loglevel = logbook.TRACE
