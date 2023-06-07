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

from collections import defaultdict
import copy
import csv
from datetime import date, timedelta
import gzip
import json
import math
from netCDF4 import Dataset
import numpy as np
import os
from pyproj import CRS, Transformer
import sqlite3
import sqlite3 as cas_sq3
import sys
import time
import zmq

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib

PATHS = {
    # adjust the local path to your environment
    "mbm-local-remote": {
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",  # mounted path to archive or hard drive with climate data
        "path-to-soil-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",  # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "remoteProducer-remoteMonica": {
        "path-to-climate-dir": "/data/",  # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",  # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-to-soil-dir": "./data/soil/global_soil_dataset_for_earth_system_modeling/",
        "path-debug-write-folder": "/out/debug-out/",
    }
}

TEMPLATE_PATH_LATLON = "{path_to_climate_dir}/latlon-to-rowcol.json"
TEMPLATE_PATH_CLIMATE_CSV = "{gcm}/{rcm}/{scenario}/{ensmem}/{version}/row-{crow}/col-{ccol}.csv"


def run_producer(server = {"server": None, "port": None}):

    context = zmq.Context()
    socket = context.socket(zmq.PUSH) # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote", ## local:"cj-local-remote" remote "mbm-local-remote"
        "server-port": server["port"] if server["port"] else "6666", ## local: 6667, remote 6666
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "start_lat": "83.95833588",
        "end_lat": "-55.95833206",
        "start_lon": "-179.95832825",
        "end_lon": "179.50000000",
        "region": "nigeria",
        "resolution": "5min",  #30sec,
        "path_to_dem_grid": "",
        "sim.json": "sim_final.json",
        "crop.json": "crop_final.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_final.csv",
        "run-setups": "[1]"
    }

    # read commandline args only if script is invoked directly from commandline
    if len(sys.argv) > 1 and __name__ == "__main__":
        for arg in sys.argv[1:]:
            k, v = arg.split("=")
            if k in config:
                config[k] = v

    print("config:", config)

    s_resolution = {"5min": 5 / 60., "30sec": 30 / 3600.}[config["resolution"]]
    s_res_scale_factor = {"5min": 60., "30sec": 3600.}[config["resolution"]]

    region_to_lat_lon_bounds = {
        "nigeria": {"tl": {"lat": 14.75, "lon": 1.75}, "br": {"lat": 3.25, "lon": 16.25}},
        "africa": {"tl": {"lat": 14.75, "lon": 1.75}, "br": {"lat": 3.25, "lon": 16.25}},
        "earth": {
            "5_min": {"tl": {"lat": 83.95833588, "lon": -179.95832825},
                      "br": {"lat": -55.95833206, "lon": 179.50000000}},
            "30_sec": {"tl": {"lat": 83.99578094, "lon": -179.99583435},
                       "br": {"lat": -55.99583435, "lon": 179.99568176}}
        }
    }

    lat_lon_bounds = region_to_lat_lon_bounds.get(config["region"], {
        "tl": {"lat": float(config["start_lat"]), "lon": float(config["start_lon"])},
        "br": {"lat": float(config["end_lat"]), "lon": float(config["end_lon"])}
    })

    # select paths
    paths = PATHS[config["mode"]]
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    #transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    #soil_crs_to_x_transformers = {}
    #wgs84_crs = CRS.from_epsg(4326)
    #utm32_crs = CRS.from_epsg(25832)
    #transformers[wgs84] = Transformer.from_crs(wgs84_crs, gk5_crs, always_xy=True)

    # Load grids
    ## note numpy is able to load from a compressed file, ending with .gz or .bz2

    # height data for germany
    #path_to_dem_grid = paths["path-to-data-dir"] + "Elevation.asc.gz"
    #dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[2])
    #dem_crs = CRS.from_epsg(dem_epsg_code)
    #if dem_crs not in soil_crs_to_x_transformers:
    #    soil_crs_to_x_transformers[dem_crs] = Transformer.from_crs(soil_crs, dem_crs)
    #dem_metadata, _ = Mrunlib.read_header(path_to_dem_grid)
    #dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    #dem_interpolate = Mrunlib.create_ascii_grid_interpolator(dem_grid, dem_metadata)
    #print("read: ", path_to_dem_grid)

    # slope data
    #path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    #slope_epsg_code = int(path_to_slope_grid.split("/")[-1].split("_")[2])
    #slope_crs = CRS.from_epsg(slope_epsg_code)
    #if slope_crs not in soil_crs_to_x_transformers:
    #    soil_crs_to_x_transformers[slope_crs] = Transformer.from_crs(soil_crs, slope_crs)
    #slope_metadata, _ = Mrunlib.read_header(path_to_slope_grid)
    #slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    #slope_interpolate = Mrunlib.create_ascii_grid_interpolator(slope_grid, slope_metadata)
    #print("read: ", path_to_slope_grid)

    # open netcdfs
    path_to_soil_netcdfs = paths["path-to-soil-dir"] + "/" + config["resolution"] + "/"
    if config["resolution"] == "5min":
        soil_data = {
            "sand": {"var": "SAND", "file": "Sand5min.nc", "conv_factor": 0.01},  # % -> fraction
            "clay": {"var": "CLAY", "file": "Clay5min.nc", "conv_factor": 1000.0},  # % -> fraction
            "corg": {"var": "OC", "file": "OC5min.nc", "conv_factor": 0.01},  # scale factor
            "bd": {"var": "BD", "file": "BD5min.nc", "conv_factor": 0.01 * 1000.0}  # scale factor * 1 g/cm3 = 1000 kg/m3
        }
    else:
        soil_data = None #["Sand5min.nc", "Clay5min.nc", "OC5min.nc", "BD5min.nc"]
    soil_datasets = {}
    soil_vars = {}
    for elem, data in soil_data.items():
        ds = Dataset(path_to_soil_netcdfs + data["file"], "r", format="NETCDF4")
        soil_datasets[elem] = ds
        soil_vars[elem] = ds.variables[data["var"]]

    def create_soil_profile(row, col):
        # skip first 4.5cm layer and just use 7 layers
        layers = []
        for i, real_depth_cm, monica_depth_m in [(0, 4.5, 0), (1, 9.1, 0.1), (2, 16.6, 0.1), (3, 28.9, 0.1),
                                                 (4, 49.3, 0.2), (5, 82.9, 0.3), (6, 138.3, 0.6), (7, 229.6, 70)][1:]:
            layers.append({
                "Thickness": [monica_depth_m, "m"],
                "SoilOrganicCarbon": [soil_vars["corg"][i, row, col] * 0.01, "%"],
                "SoilBulkDensity": [soil_vars["bd"][i, row, col] * 0.01 * soil_data["bd"]["conv_factor"], "%"],
                "Sand": [soil_vars["sand"][i, row, col] * soil_data["bd"]["conv_factor"], "%"],
                "Clay": [soil_vars["clay"][i, row, col] * soil_data["bd"]["conv_factor"], "%"]
            })
        return layers

    sent_env_count = 1
    start_time = time.perf_counter()

    # run calculations for each setup
    for _, setup_id in enumerate(run_setups):

        if setup_id not in setups:
            continue
        start_setup_time = time.perf_counter()      

        setup = setups[setup_id]
        gcm = setup["gcm"]
        scenario = setup["scenario"]
        ensmem = setup["ensmem"]

        # read template sim.json 
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"]) 

        # read template site.json
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        if len(scenario) > 0 and scenario[:3].lower() == "rcp":
            site_json["EnvironmentParameters"]["rcp"] = scenario

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup["use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

        # create environment template from json templates
        env_template = monica_io3.create_env_json_from_json_config({
            "crop": crop_json,
            "site": site_json,
            "sim": sim_json,
            "climate": ""
        })

        c_lon_0 = -179.75
        c_lat_0 = +89.25
        c_resolution = 0.5

        s_lat_0 = region_to_lat_lon_bounds["earth"][config["resolution"]]["tl"]["lat"]
        s_lon_0 = region_to_lat_lon_bounds["earth"][config["resolution"]]["tl"]["lon"]

        for lat_scaled in range(int(lat_lon_bounds["tl"]["lat"] * s_res_scale_factor),
                                int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) - 1,
                                -int(s_resolution * s_res_scale_factor)):
            lat = lat_scaled / s_res_scale_factor

            print(lat,)

            for lon_scaled in range(int(lat_lon_bounds["tl"]["lon"] * s_res_scale_factor),
                                    int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) + 1,
                                    int(s_resolution * s_res_scale_factor)):
                lon = lon_scaled / s_res_scale_factor
                print(lon,)

                c_col = int((lon - c_lon_0) / c_resolution)
                c_row = int((lat - c_lat_0) / c_resolution)

                s_col = int((lon - s_lon_0) / s_resolution)
                s_row = int((lat - s_lat_0) / s_resolution)

                soil_profile = create_soil_profile(s_row, s_col)

                height_nn = 100  #dem_interpolate(demr, demh)
                slope = 0.1  #slope_interpolate(slr, slh)

                env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup["LeafExtensionModifier"]
                    
                env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                if setup["elevation"]:
                    env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

                if setup["slope"]:
                    env_template["params"]["siteParameters"]["slope"] = slope / 100.0

                if setup["latitude"]:
                    env_template["params"]["siteParameters"]["Latitude"] = lat

                if setup["FieldConditionModifier"]:
                    env_template["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["species"]["FieldConditionModifier"] = float(setup["FieldConditionModifier"])

                env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup["fertilization"]
                env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]
                env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
                env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup["WaterDeficitResponseOn"]
                env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup["EmergenceMoistureControlOn"]
                env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup["EmergenceFloodingControlOn"]

                env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
                path_template = "{gcm}/{scenario}/{ensmem}/row-{crow}/col-{ccol}.csv.gz"

                sub_path = "isimip/3b_v1.1_CMIP6/csvs/{gcm}/{scenario}/{ensmem}/row-{crow}/col-{ccol}.csv.gz".format(
                    gcm=gcm, scenario=scenario, ensmem=ensmem, crow=c_row, ccol=c_col
                )
                env_template["pathToClimateCSV"] = paths["monica-path-to-climate-dir"] + sub_path
                print("pathToClimateCSV:", env_template["pathToClimateCSV"])

                env_template["customId"] = {
                    "setup_id": setup_id,
                    "lat": lat, "lon": lon,
                    "srow": s_row, "scol": s_col,
                    "crow": int(c_row), "ccol": int(c_col),
                    "env_id": sent_env_count,
                    "nodata": False
                }

                socket.send_json(env_template)
                print("sent env ", sent_env_count, " customId: ", env_template["customId"])

                sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup ", (sent_env_count-1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.perf_counter()

    # write summary of used json files
    try:
        print("sending ", (sent_env_count-1), " envs took ", (stop_time - start_time), " seconds")
        print("exiting run_producer()")
    except Exception:
        raise


if __name__ == "__main__":
    run_producer()
