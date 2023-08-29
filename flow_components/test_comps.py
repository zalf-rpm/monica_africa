#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg-mohnicke@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

from random import random
import capnp
from pathlib import Path
import os
import subprocess as sp
import sys
import time
from threading import Thread
import psutil

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent
if str(PATH_TO_REPO) not in sys.path:
    sys.path.insert(1, str(PATH_TO_REPO))

PATH_TO_PYTHON_CODE = PATH_TO_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from pkgs.common import capnp_async_helpers as async_helpers
from pkgs.common import common
from pkgs.climate import csv_file_based as csv_based

PATH_TO_CAPNP_SCHEMAS = PATH_TO_REPO / "capnproto_schemas"
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
reg_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "registry.capnp"), imports=abs_imports)
persistence_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "persistence.capnp"), imports=abs_imports)
climate_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "climate.capnp"), imports=abs_imports)
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
storage_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "storage.capnp"), imports=abs_imports)
geo_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "geo.capnp"), imports=abs_imports)
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)


def get_reader_writer_srs_from_channel(path_to_channel_binary, chan_name=None):
    chan = sp.Popen([
        path_to_channel_binary,
        "--name=chan_{}".format(chan_name if chan_name else str(uuid.uuid4())),
        "--output_srs",
    ], stdout=sp.PIPE, text=True)
    reader_sr = None
    writer_sr = None
    while True:
        s = chan.stdout.readline().split("=", maxsplit=1)
        id, sr = s if len(s) == 2 else (None, None)
        if id and id == "readerSR":
            reader_sr = sr.strip()
        elif id and id == "writerSR":
            writer_sr = sr.strip()
        if reader_sr and writer_sr:
            break
    return {"chan": chan, "reader_sr": reader_sr, "writer_sr": writer_sr}


async def async_main():
    config = {
        "port": "6003",
        "server": "localhost"
    }
    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    conMan = async_helpers.ConnectionManager()
    registry = await conMan.connect("capnp://insecure@localhost:9999/a8b8ff83-0af4-42c9-95c8-b6ec19a35945",
                                    registry_capnp.Registry)
    print(await registry.info().a_wait())

    yieldstat = await conMan.connect("capnp://localhost:15000", model_capnp.EnvInstance)
    info = await yieldstat.info().a_wait()
    print(info)

    time_series = await conMan.connect("capnp://localhost:11002", climate_capnp.TimeSeries)
    ts_header = (await time_series.header().a_wait()).header
    print(ts_header)

    run_req = yieldstat.run_request()
    env = run_req.env
    env.timeSeries = time_series
    env.rest = yieldstat_capnp.RestInput.new_message(
        useDevTrend=True,
        useCO2Increase=True,
        dgm=100.5,
        hft=53,
        nft=1,
        sft=36,
        slope=0,
        steino=1,
        az=14,
        klz=8,
        stt=152,
        germanFederalStates=5,  # -1
        getDryYearWaterNeed=True  # false;
    )
    cr = env.init("cropRotation", 3)
    cr[0].type = "sowing"
    cr[0].params = mgmt_capnp.Params.Sowing.new_message(cultivar="wheatWinter")
    cr[1].type = "irrigation"
    cr[2].type = "harvest"

    ys_res = (await run_req.send().a_wait()).result.as_struct(yieldstat_capnp.Output)
    print(ys_res)


def x():
    s = capnp.TwoPartyServer("*:11002",
                             bootstrap=csv_based.TimeSeries.from_csv_file("data/climate/climate-iso.csv", header_map={},
                                                                          pandas_csv_config={}))
    s.run_forever()
    s._decref()


def run_monica():
    con_man = common.ConnectionManager()
    sr = "capnp://8TwMtyGcNgiSBLXps4xRi6ymeDinAINWSrzcWJyI0Uc@10.10.24.181:41075/NzVkNzc3OTMtZjA2My00YmRkLTlkNWYtNjM2NDg1MDdjODg5"
    monica = con_man.try_connect(sr, cast_as=model_capnp.EnvInstance)
    print(monica.info().wait())
    print(monica.info().wait())
    print(monica.info().wait())
    print(monica.info().wait())



def run_channel():
    con_man = common.ConnectionManager()

    #wsr = "capnp://InIKxDrmz3tcRCEfObZRhTTuCoqpV7YpyukI3DKyyMY@10.10.25.25:42321/a0055054-7df3-493c-a5dd-ff8e64cef97f"
    #writer = con_man.try_connect(wsr, cast_as=fbp_capnp.Channel.Writer)
    #writer.write(value="hello1").wait()
    #print("wrote hello")
    #writer.write(value="hello2").wait()
    #writer.write(value="hello3").wait()
    #writer.write(value="hello4").wait()
    #return

    rsr = "capnp://InIKxDrmz3tcRCEfObZRhTTuCoqpV7YpyukI3DKyyMY@10.10.25.25:42321/0f9808d6-ac48-4fc0-bedc-de51f570081d"
    reader = con_man.try_connect(rsr, cast_as=fbp_capnp.Channel.Reader)
    msg = reader.read().wait()
    print("read", msg.value.as_text())
    return

    for _ in range(100):
        writer.write(value="hello").wait()
        print("wrote hello")
        msg = reader.read().wait()
        print("read", msg.value.as_text())
    return

    # test channel
    # channel_sr = "capnp://insecure@10.10.24.210:37505/6c25454e-4ef9-4659-9c94-341bdd827df5"
    def writer():
        conMan = common.ConnectionManager()
        writer = conMan.try_connect("capnp://insecure@10.10.24.210:43513/668ce2c1-f256-466d-99ce-30b01fd2b21b",
                                    cast_as=fbp_capnp.Channel.Writer)
        # channel = conMan.try_connect(channel_sr, cast_as=fbp_capnp.Channel)
        # writer = channel.writer().wait().w.as_interface(common_capnp.Writer)
        for i in range(1000):
            time.sleep(random())
            writer.write(value=common_capnp.X.new_message(t="hello_" + str(i))).wait()
            # writer.write(value="hello_" + str(i)).wait()
            print("wrote: hello_" + str(i))
            # writer.write(value=common_capnp.X.new_message(t="world")).wait()
        # print("wrote value:", "hello", "world")

    Thread(target=writer).start()
    reader = con_man.try_connect("capnp://2djJAQhpUZuiQxCllmwVBF86XNvrnNVw8JQnFomcBUM@10.10.24.218:33893/aW4",
                                 cast_as=fbp_capnp.Channel.Reader)
    # channel = conMan.try_connect(channel_sr, cast_as=fbp_capnp.Channel)
    # reader = channel.reader().wait().r.as_interface(common_capnp.Reader)
    for i in range(1000):
        time.sleep(random())
        print("read:", reader.read().wait().value.as_struct(common_capnp.X).t)
        # print("read:", reader.read().wait().value.as_text())
    # print(reader.read().wait().value.as_struct(common_capnp.X).t)
    # print("read value:", value)


def run_some():
    con_man = common.ConnectionManager()

    soil = con_man.try_connect("capnp://insecure@10.10.24.210:39341/9c15ad6f-0778-4bea-b91e-b015453188b9",
                               cast_as=soil_capnp.Service)
    ps = soil.profilesAt(coord={'lat': 50.02045903295569, 'lon': 8.449222632820296},
                         query={"mandatory": ["soilType", "organicCarbon", "rawDensity"]}).wait()
    print(ps)


def run_resolver():
    con_man = common.ConnectionManager()

    sr = "capnp://lpfbBZeidvkWtZXsnOO6PgNXXXHrNZi-f8yBluWkIaQ@10.10.24.250:39345"
    resolver = con_man.try_connect(sr, cast_as=persistence_capnp.HostPortResolver)
    hp = resolver.resolve("resolver").wait()
    print(hp)

def run_resolver_registrar():
    con_man = common.ConnectionManager()

    sr = "capnp://lpfbBZeidvkWtZXsnOO6PgNXXXHrNZi-f8yBluWkIaQ@10.10.24.250:39345/OGE1NjlmYWEtMDE5ZS00OTVhLWJhMGMtODNiNDJiZjZmZWZk"
    registrar = con_man.try_connect(sr, cast_as=persistence_capnp.HostPortResolver.Registrar)
    hb = registrar.register(base64VatId="lpfbBZeidvkWtZXsnOO6PgNXXXHrNZi-f8yBluWkIaQ", host="10.10.24.250", port=39345,
                            alias="resolver").wait()
    print("let heart beat every", hb.secsHeartbeatInterval, "seconds")
    def beat():
        while True:
            print("beat")
            hb.heartbeat.beat().wait()
            time.sleep(5)

    return Thread(target=beat).start()


def main():
    config = {
        "port": "6003",
        "server": "localhost",
        "path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_debug/channel",
        #"path_to_channel": "/home/rpm/start_manual_test_services/GitHub/mas-infrastructure/src/cpp/common/_cmake_release/channel",
        "path_to_python": "python",
        #"path_to_python": "/home/rpm/.conda/envs/py39/bin/python",

    }
    common.update_config(config, sys.argv, print_config=True, allow_new_keys=True)

    procs = []

    prod_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], "prod_chan")
    procs.append(prod_chan_data["chan"])
    cons_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], "cons_chan")
    procs.append(cons_chan_data["chan"])

    procs.append(sp.Popen([config["path_to_python"],
                           "run-calibration-producer.py",
                           "mode=hpc-local-remote",
                           f"server={config['server']}",
                           f"port={config['prod-port']}",
                           f"setups-file={config['setups-file']}",
                           f"run-setups={config['run-setups']}",
                           f"reader_sr={prod_chan_data['reader_sr']}",
                           f"test_mode={config['test_mode']}",
                           ] + (config["only_country_ids"] if config["only_country_ids"] else [])))

    #run_resolver()
    #hb_thread = run_resolver_registrar()

    #while True:
    #    print(".", end="")
    #    time.sleep(1)

    # run_soil_service()

    # run_channel()

    # run_crop_service()

    # run_fertilizer_service()

    # run_monica()

    # run_climate_service()

    # run_registry()

    # run_cross_domain_registry()

    # run_storage_service()

    # run_storage_container()

    return

    # s = capnp.TwoPartyServer("*:11002", bootstrap=csv_based.TimeSeries.from_csv_file("data/climate/climate-iso.csv", header_map={}, pandas_csv_config={}))
    # s.run_forever()
    # del s
    # x()

    for i in range(1):
        csv_timeseries_cap = capnp.TwoPartyClient("localhost:11002").bootstrap().cast_as(climate_capnp.TimeSeries)
        header = csv_timeseries_cap.header().wait().header
        data = csv_timeseries_cap.data().wait().data
        print("i:", i, "header:", header)

    """
    admin = connect("capnp://insecure@nb-berg-9550:10001/320a351d-c6cb-400a-92e0-4647d33cfedb", registry_capnp.Admin)
    success = admin.addCategory({"id": "models", "name": "models"}).wait().success

    soil_service = capnp.TwoPartyClient(config["server"] + ":" + config["port"]).bootstrap().cast_as(soil_data_capnp.Service)
    props = soil_service.getAllAvailableParameters().wait()
    print(props)

    profiles = soil_service.profilesAt(
        coord={"lat": 53.0, "lon": 12.5},
        query={
            "mandatory": ["sand", "clay", "bulkDensity", "organicCarbon"],
            "optional": ["pH"],
            "onlyRawData": False
        }
    ).wait().profiles
    print(profiles)

    """

    # profiles = soil_service.allLocations(
    #    mandatory=[{"sand": 0}, {"clay": 0}, {"bulkDensity": 0}, {"organicCarbon": 0}],
    #    optional=[{"pH": 0}],
    #    onlyRawData=False
    # ).wait().profiles
    # latlon_and_cap = profiles[0]  # at the moment there is always just one profile being returned
    # cap_list = latlon_and_cap.snd
    # cap = cap_list[0]
    # p = latlon_and_cap.snd[0].cap().wait().object
    # print(p)

    # soil_service = capnp.TwoPartyClient("localhost:6003").bootstrap().cast_as(soil_data_capnp.Service)
    # profiles = soil_service.profilesAt(
    #    coord={"lat": 53.0, "lon": 12.5},
    #    query={
    #        "mandatory": [{"sand": 0}, {"clay": 0}, {"bulkDensity": 0}, {"organicCarbon": 0}],
    #        "optional": [{"pH": 0}]
    #    }
    # ).wait().profiles

    # print(profiles)


if __name__ == '__main__':
    main()
    # asyncio.get_event_loop().run_until_complete(async_main()) # gets rid of some eventloop cleanup problems using te usual call below
    # asyncio.run(async_main())
