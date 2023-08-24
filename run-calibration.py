import capnp
from collections import defaultdict
import json
import csv
import matplotlib.pyplot as plt
import monica_run_lib
import os
from pathlib import Path
import spotpy
import subprocess as sp
import sys
import uuid

import calibration_spotpy_setup_MONICA

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from lib.common import common

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_MAS_INFRASTRUCTURE_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
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


def run_calibration(server=None, prod_port=None, cons_port=None):
    config = {
        "mode": "mbm-local-remote",
        "prod-port": prod_port if prod_port else "6666",  # local: 6667, remote 6666
        "cons-port": cons_port if cons_port else "7777",  # local: 6667, remote 6666
        "server": server if server else "login01.cluster.zalf.de",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_africa_calibration.csv",
        "path_to_out": "out/",
        "run-setups": "[1]",
        #"path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_debug/channel",
        "path_to_channel": "/home/rpm/start_manual_test_services/GitHub/mas-infrastructure/src/cpp/common/_cmake_release/channel",
        #"path_to_python": "python",
        "path_to_python": "/home/rpm/.conda/envs/py39/bin/python",
        "repetitions": "200",
        "test_mode": "false",
        "only_country_ids": None  # "[10]",
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    procs = []

    prod_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], "prod_chan")
    procs.append(prod_chan_data["chan"])
    cons_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], "cons_chan")
    procs.append(cons_chan_data["chan"])

    procs.append(sp.Popen([
        config["path_to_python"],
        "run-calibration-producer.py",
        "mode=hpc-local-remote",
        f"server={config['server']}",
        f"port={config['prod-port']}",
        f"setups-file={config['setups-file']}",
        f"run-setups={config['run-setups']}",
        f"reader_sr={prod_chan_data['reader_sr']}",
        f"test_mode={config['test_mode']}",
    ] + (config["only_country_ids"] if config["only_country_ids"] else [])))

    procs.append(sp.Popen([
        config["path_to_python"],
        "run-calibration-consumer.py",
        # "mode=remoteConsumer-remoteMonica",
        f"server={config['server']}",
        f"port={config['cons-port']}",
        f"run-setups={config['run-setups']}",
        f"writer_sr={cons_chan_data['writer_sr']}",
    ]))

    crop_to_observations = defaultdict(list)
    with open("data/FAO_yield_data.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            crop_to_observations[row[0].strip().lower()].append(
                {"id": int(row[4]),
                 "year": int(row[2]),
                 "value": float(row[3])*1000.0  # t/ha -> kg/ha
                 })

    # order obslist by exp_id to avoid mismatch between observation/evaluation lists
    for crop, obs in crop_to_observations.items():
        obs.sort(key=lambda r: [r["id"], r["year"]])

    # read parameters which are to be calibrated
    params = []
    with open("calibratethese.csv") as params_csv:
        dialect = csv.Sniffer().sniff(params_csv.read(), delimiters=';,\t')
        params_csv.seek(0)
        reader = csv.reader(params_csv, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            p = {"name": row[0]}
            if len(row[1]) > 0:
                p["array"] = int(row[1])
            for n, i in [("low", 2), ("high", 3), ("step", 4), ("optguess", 5), ("minbound", 6), ("maxbound", 7)]:
                if len(row[i]) > 0:
                    p[n] = float(row[i])
            if len(row) == 9 and len(row[8]) > 0:
                p["derive_function"] = lambda _, _2: eval(row[8])
            params.append(p)

    con_man = common.ConnectionManager()

    setups = monica_run_lib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    if len(config["run-setups"]) < 1:
        return
    setup_id = run_setups[0]
    setup = setups[setup_id]
    cons_reader = con_man.try_connect(cons_chan_data["reader_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
    prod_writer = con_man.try_connect(prod_chan_data["writer_sr"], cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

    # configure MONICA setup for spotpy
    observations = crop_to_observations[setup["crop"]]
    if config["only_country_ids"]:
        only_country_ids = json.loads(config["only_country_ids"])
        observations = list(filter(lambda d: d["id"] in only_country_ids, observations))
    spot_setup = calibration_spotpy_setup_MONICA.spot_setup(params, observations, prod_writer, cons_reader)

    rep = int(config["repetitions"]) #initial number was 10
    results = []
    #Set up the sampler with the model above
    sampler = spotpy.algorithms.sceua(spot_setup, dbname='SCEUA_monica_results', dbformat='csv')

    #Run the sampler to produce the paranmeter distribution
    #and identify optimal parameters based on objective function
    #ngs = number of complexes
    #kstop = max number of evolution loops before convergence
    #peps = convergence criterion
    #pcento = percent change allowed in kstop loops before convergence
    sampler.sample(rep, ngs=len(params)*2, peps=0.001, pcento=0.001)

    #Extract the parameter samples from distribution
    results = spotpy.analyser.load_csv_results("SCEUA_monica_results")

    # Plot how the objective function was minimized during sampling
    #font = {"family": "calibri",
    #        "weight": "normal",
    #        "size": 18}
    fig = plt.figure(1, figsize=(9, 6))
    #plt.plot(results["like1"],  marker='o')
    plt.plot(results["like1"], "r+")
    plt.show()
    plt.ylabel("RMSE")
    plt.xlabel("Iteration")
    fig.savefig(f"{config['path_to_out']}/SCEUA_objectivefunctiontrace_MONICA.png", dpi=150)

    # kill the two channels and the producer and consumer
    for proc in procs:
        proc.terminate()

    print("sampler_MONICA.py finished")


if __name__ == "__main__":
    run_calibration()


