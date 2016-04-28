#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @author: zig(shawhen2012@hotmail.com)

import argparse
import threading
import subprocess

import flask
from flask import request

from gevent import pywsgi

import main as m_main


parser = argparse.ArgumentParser()
parser.add_argument("--lcx_host", default="u238r1.uranome.com", help="lcx host(ip or domain)")
parser.add_argument("--lcx_port", default=30092, help="lcx port")
args = parser.parse_args()

# start lcx slave
slavep = None

# wait for curl config
app = flask.Flask(__name__)
app.debug = True

dbop_server_p = None


@app.route("/setup", methods=["POST"])
def setup():
    global dbop_server_p
    global slavep

    print("form:", request.form)
    try:
        node = request.form["node"]
        db_host = request.form["db_host"]
        db_port = int(request.form["db_port"])
        listen_host = request.form.get("listen_host", "localhost")
        listen_port = int(request.form.get("listen_port", 32001))

        if dbop_server_p is not None:
            dbop_server_p.terminate()
        dbop_server_p = subprocess.Popen("python3.4 main.py --listen_host {listen_host} --listen_port {listen_port} --node {node} --dbhost {dbhost} --dbport {dbport}"
                                         .format(listen_host=listen_host, listen_port=listen_port, node=node, dbhost=db_host, dbport=db_port), shell=True)

        if slavep is not None:
            slavep.terminate()
        slavep = subprocess.Popen("python3.4 slave.py --host {host} --port {port} --whost localhost --wport 22 --type red --iid {iid}"
                                  .format(host=args.lcx_host, port=args.lcx_port, iid="ssh_dr_docker_dbop_redis"+"_"+str(listen_port)),
                                  cwd="slave", shell=True)
    except:
        import traceback
        traceback.print_exc()

        raise
    return "ok"


curl_server = pywsgi.WSGIServer(("0.0.0.0", 49977), app)
curl_server.serve_forever()
