import capnp
from collections import defaultdict
import json
import calibration_spotpy_setup_MONICA
import csv
from datetime import date
from matplotlib import colors
import matplotlib.pyplot as plt
import monica_run_lib
import numpy as np
import os
from pathlib import Path
import spotpy
import sys

import common.common as common

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)

def run_calibration(server={"server": None, "port": None}):
    config = {
        "mode": "mbm-local-remote",
        "server-port": server["port"] if server["port"] else "6666",  # local: 6667, remote 6666
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_africa.csv",
        "run-setups": "[1]",
        "out_sr": "capnp://OLFKGVRKGOanC9Egq_i5wwr0GG8zEuizBLU1f_UIR9w@10.10.25.25:46815/a87beca7-a0ef-4fc3-8174-e9a6cc669e9e",
        "in_sr": "capnp://2c4klhrLPxOrp8_Azm0ojkC-28iAQ4I9XZoKUUAGlPw@10.10.25.25:36413/c1ad5f00-ddf1-4105-b268-ca804e564ad9"
    }

    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    crop_to_observations = defaultdict(list)
    with open("data/FAO_yield_data.csv") as file:
        dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
        file.seek(0)
        reader = csv.reader(file, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            crop_to_observations[row[0].lower()].append(
                {"id": int(row[4]),
                 "year": int(row[2]),
                 "value": float(row[3])})

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

    conman = common.ConnectionManager()

    setups = monica_run_lib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    if len(config["run-setups"]) < 1:
        return
    setup_id = run_setups[0]
    setup = setups[setup_id]
    cons_inp = conman.try_connect(config["out_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
    prod_outp = conman.try_connect(config["in_sr"], cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

    #Here, MONICA is initialized and a producer is started:
    #Arguments are: Parameters, Sites, Observations
    #Returns a ready made setup
    obs_list = list(map(lambda d: d["value"], crop_to_observations[setup["crop"]]))
    spot_setup = calibration_spotpy_setup_MONICA.spot_setup(params, obs_list, prod_outp, cons_inp)

    rep = 200 #initial number was 10
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
    font = {'family': 'calibri',
            'weight': 'normal',
            'size': 18}
    fig = plt.figure(1, figsize=(9, 6))
    #plt.plot(results["like1"],  marker='o')
    plt.plot(results["like1"],  'r+')
    plt.show()
    plt.ylabel("RMSE")
    plt.xlabel("Iteration")
    fig.savefig("SCEUA_objectivefunctiontrace_MONICA.png", dpi=150)


    print("sampler_MONICA.py finished")

if __name__ == "__main__":
    run_calibration()


