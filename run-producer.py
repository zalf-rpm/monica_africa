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

from datetime import date, timedelta
import json
import numpy as np
import os
from pathlib import Path
import sys
import time
import zmq

import monica_run_lib
import shared

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from pkgs.common import common
from pkgs.model import monica_io3

PATHS = {
    # adjust the local path to your environment
    "mbm-local-local": {
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        # "path-to-soil-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        "path-to-soil-dir": "/home/berg/Desktop/soil/",
        "monica-path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "mbm-local-remote": {
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        # "path-to-soil-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        "path-to-soil-dir": "/home/berg/Desktop/soil/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "hpc-local-remote": {
        #"path-to-climate-dir": "/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        # mounted path to archive or hard drive with climate data
        "path-to-soil-dir": "/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-debug-write-folder": "./debug-out/",
    },
    "remoteProducer-remoteMonica": {
        "path-to-climate-dir": "/data/",  # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
        "path-to-soil-dir": "/project/soil/global_soil_dataset_for_earth_system_modeling/",
        "path-debug-write-folder": "/out/debug-out/",
    }
}


def run_producer(server={"server": None, "port": None}):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member

    config = {
        "mode": "mbm-local-remote",
        "server-port": server["port"] if server["port"] else "6666",  # local: 6667, remote 6666
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "start_lat": "83.95833588",
        "end_lat": "-55.95833206",
        "start_lon": "-179.95832825",
        "end_lon": "179.50000000",
        "region": "africa",
        "resolution": "5min",  # 30sec,
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_africa.csv",
        "run-setups": "[2]"
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    s_resolution = {"5min": 5 / 60., "30sec": 30 / 3600.}[config["resolution"]]
    s_res_scale_factor = {"5min": 60., "30sec": 3600.}[config["resolution"]]

    region_to_lat_lon_bounds = {
        "nigeria": {"tl": {"lat": 14.0, "lon": 2.7}, "br": {"lat": 4.25, "lon": 14.7}},
        "africa": {"tl": {"lat": 37.4, "lon": -17.55}, "br": {"lat": -34.9, "lon": 51.5}},
        "earth": {
            "5min": {"tl": {"lat": 83.95833588, "lon": -179.95832825},
                     "br": {"lat": -55.95833206, "lon": 179.50000000}},
            "30sec": {"tl": {"lat": 83.99578094, "lon": -179.99583435},
                      "br": {"lat": -55.99583435, "lon": 179.99568176}}
        }
    }

    # select paths
    paths = PATHS[config["mode"]]
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # read setup from csv file
    setups = monica_run_lib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    # transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    # soil_crs_to_x_transformers = {}
    # wgs84_crs = CRS.from_epsg(4326)
    # utm32_crs = CRS.from_epsg(25832)
    # transformers[wgs84] = Transformer.from_crs(wgs84_crs, gk5_crs, always_xy=True)

    # eco regions
    path_to_eco_grid = (paths["path-to-data-dir"] +
                        "/agro_ecological_regions_nigeria/agro-eco-regions_0.038deg_4326_wgs84_nigeria.asc")
    eco_metadata, _ = monica_run_lib.read_header(path_to_eco_grid)
    eco_grid = np.loadtxt(path_to_eco_grid, dtype=int, skiprows=len(eco_metadata))
    aer_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(eco_metadata)

    global_soil_dataset = shared.GlobalSoilDataSet(paths["path-to-soil-dir"], config["resolution"])

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
        crop = setup["crop"]

        region = setup["region"] if "region" in setup else config["region"]
        lat_lon_bounds = region_to_lat_lon_bounds.get(region, {
            "tl": {"lat": float(config["start_lat"]), "lon": float(config["start_lon"])},
            "br": {"lat": float(config["end_lat"]), "lon": float(config["end_lon"])}
        })

        if setup["region"] == "nigeria":
            planting = setup["planting"].lower()
            nitrogen = setup["nitrogen"].lower()
            management_file = f"{planting}_planting_{nitrogen}_nitrogen.csv"
            # load management data
            management = monica_run_lib.read_csv(paths["path-to-data-dir"] +
                                          "/agro_ecological_regions_nigeria/" + management_file, key="id")
        else:
            planting = nitrogen = management = None

        path_to_planting_grid = \
            paths["path-to-data-dir"] + f"/{setup['crop']}-planting-doy_0.5deg_4326_wgs84_africa.asc"
        planting_metadata, _ = monica_run_lib.read_header(path_to_planting_grid)
        planting_grid = np.loadtxt(path_to_planting_grid, dtype=int, skiprows=len(planting_metadata))
        # print("read: ", path_to_planting_grid)
        planting_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(planting_metadata)

        path_to_harvest_grid = \
            paths["path-to-data-dir"] + f"/{setup['crop']}-harvest-doy_0.5deg_4326_wgs84_africa.asc"
        harvest_metadata, _ = monica_run_lib.read_header(path_to_harvest_grid)
        harvest_grid = np.loadtxt(path_to_harvest_grid, dtype=int, skiprows=len(harvest_metadata))
        # print("read: ", path_to_harvest_grid)
        harvest_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(harvest_metadata)

        # height data for germany
        path_to_dem_grid = setup["path_to_dem_asc_grid"]
        dem_metadata, _ = monica_run_lib.read_header(path_to_dem_grid)
        dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=len(dem_metadata))
        # print("read: ", path_to_dem_grid)
        dem_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(dem_metadata)

        # slope data
        path_to_slope_grid = setup["path_to_slope_asc_grid"]
        slope_metadata, _ = monica_run_lib.read_header(path_to_slope_grid)
        slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=len(slope_metadata))
        print("read: ", path_to_slope_grid)
        slope_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(slope_metadata)

        # read template sim.json
        with open(setup.get("sim.json", config["sim.json"])) as _:
            sim_json = json.load(_)
        # change start and end date acording to setup
        if setup["start_date"]:
            sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
        if setup["end_date"]:
            end_year = int(setup["end_date"].split("-")[0])
            sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])

            # read template site.json
        with open(setup.get("site.json", config["site.json"])) as _:
            site_json = json.load(_)

        if len(scenario) > 0 and scenario[:3].lower() == "ssp":
            site_json["EnvironmentParameters"]["rcp"] = f"rcp{scenario[-2:]}"

        # read template crop.json
        with open(setup.get("crop.json", config["crop.json"])) as _:
            crop_json = json.load(_)
            # set current crop
            for ws in crop_json["cropRotation"][0]["worksteps"]:
                if "Sowing" in ws["type"]:
                    ws["crop"][2] = crop

        crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup[
            "use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

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
        b_lat_0 = lat_lon_bounds["tl"]["lat"]
        b_lon_0 = lat_lon_bounds["tl"]["lon"]

        lats_scaled = range(int(lat_lon_bounds["tl"]["lat"] * s_res_scale_factor),
                            int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) - 1,
                            -int(s_resolution * s_res_scale_factor))
        no_of_lats = len(lats_scaled)
        s_row_0 = int((s_lat_0 - (lats_scaled[0] / s_res_scale_factor)) / s_resolution)
        for lat_scaled in lats_scaled:
            lat = lat_scaled / s_res_scale_factor

            print(lat, )

            lons_scaled = range(int(lat_lon_bounds["tl"]["lon"] * s_res_scale_factor),
                                int(lat_lon_bounds["br"]["lon"] * s_res_scale_factor) + 1,
                                int(s_resolution * s_res_scale_factor))
            no_of_lons = len(lons_scaled)
            s_col_0 = int(((lons_scaled[0] / s_res_scale_factor) - s_lon_0) / s_resolution)
            for lon_scaled in lons_scaled:
                lon = lon_scaled / s_res_scale_factor
                print(lon, )

                c_col = int((lon - c_lon_0) / c_resolution)
                c_row = int((c_lat_0 - lat) / c_resolution)

                s_col = int((lon - s_lon_0) / s_resolution)
                s_row = int((s_lat_0 - lat) / s_resolution)

                # set management
                mgmt = None
                aer = None
                if setup["region"] == "nigeria":
                    aer_col = int((lon - aer_ll0r["lon_0"]) / aer_ll0r["res"])
                    aer_row = int((aer_ll0r["lat_0"] - lat) / aer_ll0r["res"])
                    if 0 <= aer_row < int(eco_metadata["nrows"]) \
                            and 0 <= aer_col < int(eco_metadata["ncols"]):
                        aer = eco_grid[aer_row, aer_col]
                        if aer > 0 and aer in management:
                            mgmt = management[aer]
                else:
                    mgmt = {}

                    planting_col = int((lon - planting_ll0r["lon_0"]) / planting_ll0r["res"])
                    planting_row = int((planting_ll0r["lat_0"] - lat) / planting_ll0r["res"])
                    if 0 <= planting_row < int(planting_metadata["nrows"]) \
                            and 0 <= planting_col < int(planting_metadata["ncols"]):
                        planting_doy = int(planting_grid[planting_row, planting_col])
                        if planting_doy != planting_metadata["nodata_value"]:
                            d = date(2023, 1, 1) + timedelta(days=planting_doy-1)
                            mgmt["Sowing date"] = f"0000-{d.month:02}-{d.day:02}"

                    harvest_col = int((lon - harvest_ll0r["lon_0"]) / harvest_ll0r["res"])
                    harvest_row = int((harvest_ll0r["lat_0"] - lat) / harvest_ll0r["res"])
                    if 0 <= harvest_row < int(harvest_metadata["nrows"]) \
                            and 0 <= harvest_col < int(harvest_metadata["ncols"]):
                        harvest_doy = int(harvest_grid[harvest_row, harvest_col])
                        if harvest_doy != harvest_metadata["nodata_value"]:
                            d = date(2023, 1, 1) + timedelta(days=harvest_doy - 1)
                            mgmt["Harvest date"] = f"0000-{d.month:02}-{d.day:02}"

                valid_mgmt = False
                if mgmt and shared.check_for_nill_dates(mgmt) and len(mgmt) > 1:
                    valid_mgmt = True
                    for ws in env_template["cropRotation"][0]["worksteps"]:
                        if ws["type"] == "Sowing" and "Sowing date" in mgmt:
                            ws["date"] = shared.mgmt_date_to_rel_date(mgmt["Sowing date"])
                            if "Planting density" in mgmt:
                                ws["PlantDensity"] = [float(mgmt["Planting density"]), "plants/m2"]
                        elif ws["type"] == "AutomaticHarvest" and "Harvest date" in mgmt:
                            ws["latest-date"] = shared.mgmt_date_to_rel_date(mgmt["Harvest date"])
                        elif ws["type"] == "Tillage" and "Tillage date" in mgmt:
                            ws["date"] = shared.mgmt_date_to_rel_date(mgmt["Tillage date"])
                        elif ws["type"] == "MineralFertilization" and mgmt[:2] == "N " and mgmt[-5:] == " date":
                            app_no = int(ws["application"])
                            app_str = str(app_no) + ["st", "nd", "rd", "th"][app_no - 1]
                            ws["date"] = shared.mgmt_date_to_rel_date(mgmt[f"N {app_str} date"])
                            ws["amount"] = [float(mgmt[f"N {app_str} application (kg/ha)"]), "kg"]
                else:
                    mgmt = None

                def send_nodata_msg(sec):
                    env_template["customId"] = {
                        "setup_id": setup_id,
                        "lat": lat, "lon": lon,
                        "b_lat_0": b_lat_0, "b_lon_0": b_lon_0,
                        "s_resolution": s_resolution,
                        "s_row": s_row, "s_col": s_col,
                        "s_row_0": s_row_0, "s_col_0": s_col_0,
                        "no_of_s_cols": no_of_lons, "no_of_s_rows": no_of_lats,
                        "c_row": int(c_row), "c_col": int(c_col),
                        "env_id": sec,
                        "planting": planting,
                        "nitrogen": nitrogen,
                        "region": region,
                        "crop": crop,
                        "nodata": True
                    }
                    socket.send_json(env_template)
                    print("sent nodata env ", sec, " customId: ", env_template["customId"])

                if mgmt is None or not valid_mgmt:
                    send_nodata_msg(sent_env_count)
                    sent_env_count += 1
                    continue

                soil_profile = global_soil_dataset.create_soil_profile(s_row, s_col)
                if not soil_profile:
                    send_nodata_msg(sent_env_count)
                    sent_env_count += 1
                    continue

                dem_col = int((lon - dem_ll0r["lon_0"]) / dem_ll0r["res"])
                dem_row = int((dem_ll0r["lat_0"] - lat) / dem_ll0r["res"])
                height_nn = dem_grid[dem_row, dem_col]
                if height_nn == dem_metadata["nodata_value"]:
                    send_nodata_msg(sent_env_count)
                    sent_env_count += 1
                    continue

                slope_col = int((lon - slope_ll0r["lon_0"]) / slope_ll0r["res"])
                slope_row = int((slope_ll0r["lat_0"] - lat) / slope_ll0r["res"])
                slope = slope_grid[slope_row, slope_col]
                if slope == slope_metadata["nodata_value"]:
                    slope = 0

                env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup[
                    "LeafExtensionModifier"]

                env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                if setup["elevation"]:
                    env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

                if setup["slope"]:
                    if setup["slope_unit"] == "degree":
                        s = slope / 90.0
                    else:
                        s = slope
                    env_template["params"]["siteParameters"]["slope"] = s

                if setup["latitude"]:
                    env_template["params"]["siteParameters"]["Latitude"] = lat

                if setup["FieldConditionModifier"]:
                    for ws in env_template["cropRotation"][0]["worksteps"]:
                        if "Sowing" in ws["type"]:
                            if "|" in setup["FieldConditionModifier"] and aer and aer > 0:
                                fcms = setup["FieldConditionModifier"].split("|")
                                fcm = float(fcms[aer-1])
                                if fcm > 0:
                                    ws["crop"]["cropParams"]["species"]["FieldConditionModifier"] = fcm
                            else:
                                ws["crop"]["cropParams"]["species"]["FieldConditionModifier"] = \
                                    setup["FieldConditionModifier"]

                env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup[
                    "fertilization"]
                env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]
                env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]
                env_template["params"]["simulationParameters"]["WaterDeficitResponseOn"] = setup[
                    "WaterDeficitResponseOn"]
                env_template["params"]["simulationParameters"]["EmergenceMoistureControlOn"] = setup[
                    "EmergenceMoistureControlOn"]
                env_template["params"]["simulationParameters"]["EmergenceFloodingControlOn"] = setup[
                    "EmergenceFloodingControlOn"]

                env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]
                hist_sub_path = "isimip/3b_v1.1_CMIP6/csvs/{gcm}/historical/{ensmem}/row-{crow}/col-{ccol}.csv.gz".format(
                    gcm=gcm, ensmem=ensmem, crow=c_row, ccol=c_col)
                sub_path = "isimip/3b_v1.1_CMIP6/csvs/{gcm}/{scenario}/{ensmem}/row-{crow}/col-{ccol}.csv.gz".format(
                    gcm=gcm, scenario=scenario, ensmem=ensmem, crow=c_row, ccol=c_col
                )
                if setup["incl_historical"] and scenario != "historical":
                    climate_data_paths = [
                        paths["monica-path-to-climate-dir"] + hist_sub_path,
                        paths["monica-path-to-climate-dir"] + sub_path
                    ]
                else:
                    climate_data_paths = [paths["monica-path-to-climate-dir"] + sub_path]
                env_template["pathToClimateCSV"] = climate_data_paths
                print("pathToClimateCSV:", env_template["pathToClimateCSV"])

                env_template["customId"] = {
                    "setup_id": setup_id,
                    "lat": lat, "lon": lon,
                    "s_lat_0": s_lat_0, "s_lon_0": s_lon_0,
                    "s_resolution": s_resolution,
                    "s_row": s_row, "s_col": s_col,
                    "s_row_0": s_row_0, "s_col_0": s_col_0,
                    "no_of_s_cols": no_of_lons, "no_of_s_rows": no_of_lats,
                    "c_row": int(c_row), "c_col": int(c_col),
                    "env_id": sent_env_count,
                    "planting": planting,
                    "nitrogen": nitrogen,
                    "region": region,
                    "crop": crop,
                    "nodata": False
                }

                socket.send_json(env_template)
                print("sent env ", sent_env_count, " customId: ", env_template["customId"])

                sent_env_count += 1

        stop_setup_time = time.perf_counter()
        print("Setup ", (sent_env_count - 1), " envs took ", (stop_setup_time - start_setup_time), " seconds")

    stop_time = time.perf_counter()

    # write summary of used json files
    try:
        print("sending ", (sent_env_count - 1), " envs took ", (stop_time - start_time), " seconds")
        print("exiting run_producer()")
    except Exception:
        raise


if __name__ == "__main__":
    run_producer()
