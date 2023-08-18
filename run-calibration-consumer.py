#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import capnp
from collections import defaultdict, OrderedDict
import csv
from datetime import datetime
import gc
import json
import numpy as np
import os
from pathlib import Path
from pyproj import CRS, Transformer
import sqlite3
import sys
import timeit
import types
import zmq

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib

import common.common as common

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)

PATHS = {
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}

def run_consumer(leave_after_finished_run=True, server={"server": None, "port": None}):
    """collect data from workers"""

    config = {
        "mode": "remoteConsumer-remoteMonica",
        "port": server["port"] if server["port"] else "7777",  # local 7778,  remote 7777
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "writer_sr": "capnp://OU0P6_fB84Vrdq2bdb5nJqMk9sux4z5640XrAlAf9pg@10.10.25.25:40023/1115e763-2fd2-4da2-b8d5-bb64caf33e4f",
        "timeout": 600000  # 10min
    }

    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    paths = PATHS[config["mode"]]

    print("consumer config:", config)

    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]

    country_id_to_year_to_yields = defaultdict(lambda: defaultdict(list))

    conman = common.ConnectionManager()
    writer = conman.try_connect(config["writer_sr"], cast_as=fbp_capnp.Channel.Writer, retry_secs=1)  #None

    envs_received = 0
    no_of_envs_expected = None

    while True:
        try:
            msg: dict = socket.recv_json()  # encoding="latin-1"

            custom_id = msg["customId"]
            if "no_of_sent_envs" in custom_id:
                no_of_envs_expected = custom_id["no_of_sent_envs"]
                continue
            else:
                envs_received += 1

            leave = no_of_envs_expected == envs_received

            #writer_sr = custom_id["writer_sr"]
            country_id = custom_id["country_id"]

            #if writer is None:
            #    writer = conman.try_connect(writer_sr, cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

            for data in msg.get("data", []):
                results = data.get("results", [])
                for vals in results:
                    if "Year" in vals:
                        country_id_to_year_to_yields[country_id][int(vals["Year"])].append(vals["Yield"])

            if no_of_envs_expected == envs_received and writer:
                list_of_country_id_and_year_and_avg_yield = []
                for country_id, rest in country_id_to_year_to_yields.items():
                    for year, yields in rest.items():
                        if len(yields) > 0:
                            list_of_country_id_and_year_and_avg_yield.append(
                                {"id": country_id, "year": year, "value": sum(yields) / len(yields)})

                out_ip = fbp_capnp.IP.new_message(content=json.dumps(list_of_country_id_and_year_and_avg_yield))
                writer.write(value=out_ip).wait()

                # reset and wait for next round
                country_id_to_year_to_yields.clear()
                no_of_envs_expected = None
                envs_received = 0

        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            continue
        except Exception as e:
            print("Exception:", e)
            break

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
