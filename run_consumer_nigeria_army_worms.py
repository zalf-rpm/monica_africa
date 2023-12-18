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
import csv
import numpy as np
import os
from pathlib import Path
import sys
import zmq
from datetime import datetime
import shared

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

# for hpc singularity use
PATH_TO_PYTHON_CODE = "/mas-infrastructure/src/python"
if PATH_TO_PYTHON_CODE not in sys.path:
    sys.path.insert(1, PATH_TO_PYTHON_CODE)

#from pkgs.common import common
from pkgs.model import monica_io3

PATHS = {
    "mbm-local-remote": {
        "path-to-data-dir": "data/",
        "path-to-output-dir": "out/",
        "path-to-csv-output-dir": "csv-out/"
    },
    "remoteConsumer-remoteMonica": {
        "path-to-data-dir": "./data/",
        "path-to-output-dir": "/out/out/",
        "path-to-csv-output-dir": "/out/csv-out/"
    }
}


def calculate_index_data(msg):
    year_to_worm_index_info = defaultdict(lambda: {
        "year": None,
        "crop": "none",
        "worm_index": 0,
        "window_count": 0,
    })
    cm_count_to_worm_index_info = defaultdict(lambda: {
        "year": None,
        "crop": None,
        "worm_index": 0,
        "window_count": 0,
    })

    cm_count_to_season_info = defaultdict(lambda: {
        "year": None,
        "sowing_doy": 0,
        "harvest_doy": 0,
    })

    cm_count_to_stresses = defaultdict(lambda: {
        "year": None,
        "crop": None,
        "dry": 0,
        "dry_and_hot": 0,
        "dry_and_cold": 0,
        "wet": 0,
        "wet_and_hot": 0,
        "wet_and_cold": 0,
        "cold": 0,
        "hot": 0,
    })

    #all_years = set()
    #all_cm_counts = set()

    for data in msg.get("data", []):
        results = data.get("results", [])

        is_daily_section = data.get("origSpec", "") == '"daily"'
        is_crop_section = data.get("origSpec", "") == '"crop"'

        days_in_window = 0
        for vals in results:
            if "CM-count" not in vals or "year" not in vals:
                continue

            if is_crop_section:
                #all_cm_counts.add(vals["CM-count"])
                cm_count_to_season_info[vals["CM-count"]]["year"] = vals["year"]
                cm_count_to_season_info[vals["CM-count"]]["sowing_doy"] = vals["sowing_doy"]
                cm_count_to_season_info[vals["CM-count"]]["harvest_doy"] = vals["harvest_doy"]
            elif is_daily_section:
                #all_years.add(vals["year"])
                date = datetime.fromisoformat(vals["Date"])
                doy = date.timetuple().tm_yday
                s_doy = cm_count_to_season_info[vals["CM-count"]]["sowing_doy"]
                h_doy = cm_count_to_season_info[vals["CM-count"]]["harvest_doy"]

                sm = vals["sm_0-10"][0]
                tmin = vals["tmin"]
                tmax = vals["tmax"]
                tavg = vals["tavg"]
                year = vals["year"]
                cmc = vals["CM-count"]
                crop = vals.get("crop", "none")

                wii_year = year_to_worm_index_info[year]
                if wii_year["year"] is None:
                    wii_year["year"] = vals["year"]
                wii_cmc = cm_count_to_worm_index_info[cmc]
                if wii_cmc["year"] is None:
                    wii_cmc["year"] = vals["year"]
                if wii_cmc["crop"] is None and len(crop) > 0:
                    wii_cmc["crop"] = crop
                stresses_cmc = cm_count_to_stresses[cmc]
                if stresses_cmc["year"] is None:
                    stresses_cmc["year"] = vals["year"]
                if stresses_cmc["crop"] is None and len(crop) > 0:
                    stresses_cmc["crop"] = crop

                # breeding condition met
                if 0.15 <= sm <= 0.2 and 25 <= tmax <= 36:
                    days_in_window += 1

                    if days_in_window == 7:
                        wii_year["worm_index"] += 1
                        wii_year["window_count"] += 1
                    elif days_in_window > 7:
                        wii_year["worm_index"] += 1./7.

                    if s_doy <= doy <= h_doy:
                        if days_in_window == 7:
                            wii_cmc["worm_index"] += 1
                            wii_cmc["window_count"] += 1
                        elif days_in_window > 7:
                            wii_cmc["worm_index"] += 1./7.

                # stress conditions might apply
                else:
                    days_in_window = 0

                    if s_doy <= doy <= h_doy:
                        if stresses_cmc["crop"] is None:
                            stresses_cmc["crop"] = vals["crop"]

                        dry = sm < 0.15
                        wet = sm > 0.2
                        cold = tmin < 15
                        hot = tmax > 36

                        if dry:
                            stresses_cmc["dry"] += 1
                            if hot:
                                stresses_cmc["dry_and_hot"] += 1
                            elif cold:
                                stresses_cmc["dry_and_cold"] += 1
                        elif wet:
                            stresses_cmc["wet"] += 1
                            if hot:
                                stresses_cmc["wet_and_hot"] += 1
                            elif cold:
                                stresses_cmc["wet_and_cold"] += 1
                        elif hot:
                            stresses_cmc["hot"] += 1
                        elif cold:
                            stresses_cmc["cold"] += 1

    cm_count_to_vals = defaultdict(dict)
    for y, i in year_to_worm_index_info.items():
        cm_count_to_vals[y] = i
    for cmc, i in cm_count_to_worm_index_info.items():
        if cmc in cm_count_to_vals:
            cm_count_to_vals[cmc].update(i)
        else:
            cm_count_to_vals[cmc] = i
    for cmc, i in cm_count_to_stresses.items():
        if cmc in cm_count_to_vals:
            cm_count_to_vals[cmc].update(i)
        else:
            cm_count_to_vals[cmc] = i

    # fill in years and cm_counts which produced no data
    #for cmc in all_cm_counts:
    #    if cmc not in cm_count_to_vals:
    #        cm_count_to_vals[cmc] = {"year": cm_count_to_season_info[cmc]["year"]}
    #for year in all_years:
    #    if year not in cm_count_to_vals:
    #        cm_count_to_vals[year] = {"year": year}

    # remove cm_count=0, which we don't need
    cm_count_to_vals.pop(0, None)

    return cm_count_to_vals


def write_row_to_grids(row_col_data, row, col_0, no_of_cols, header, path_to_output_dir, setup_id):
    """write grids row by row"""

    if not hasattr(write_row_to_grids, "nodata_row_count"):
        write_row_to_grids.nodata_row_count = defaultdict(lambda: 0)
        write_row_to_grids.list_of_output_files = defaultdict(list)

    def make_dict_nparr():
        return defaultdict(lambda: np.full((no_of_cols,), -9999, dtype=float))
    def make_dict_nparr_int():
        return defaultdict(lambda: np.full((no_of_cols,), -9999, dtype=int))

    output_grids = {
        # "Evapotranspiration": {"data": make_dict_nparr(), "cast-to": "float", "digits": 2},
        "worm_index": {"data": make_dict_nparr(), "cast-to": "float", "digits": 2},
        "window_count": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "dry": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "wet": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "hot": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "cold": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "dry_and_hot": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "dry_and_cold": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "wet_and_hot": {"data": make_dict_nparr_int(), "cast-to": "int"},
        "wet_and_cold": {"data": make_dict_nparr_int(), "cast-to": "int"},
    }
    
    output_keys = list(output_grids.keys())

    cmc_to_crop = {}

    is_no_data_row = True
    # skip this part if we write just a nodata line
    if row in row_col_data:
        no_data_cols = no_of_cols
        for col in range(col_0, col_0 + no_of_cols + 1):
            if col in row_col_data[row]:
                rcd_val = row_col_data[row][col]
                if rcd_val == -9999:
                    continue
                else:
                    no_data_cols -= 1
                    cmc_and_year_to_vals = defaultdict(lambda: defaultdict(list))
                    for cell_data in rcd_val:
                        # if we got multiple datasets per cell, iterate over them and aggregate them in the following step
                        for cm_count, data in cell_data.items():
                            # store mapping cm_count to crop name for later file name creation
                            if cm_count not in cmc_to_crop and "crop" in data:
                                cmc_to_crop[cm_count] = data["crop"]

                            for key in output_keys:
                                # only further process/store data we actually received
                                if key in data:
                                    v = data[key]
                                    if isinstance(v, list):
                                        requires_sum = len(v) > 1
                                        for i, v_ in enumerate(v):
                                            if requires_sum:
                                                cmc_and_year_to_vals[(cm_count, data["year"])][f"{key}_{i + 1}"].append(v_)
                                            else:
                                                cmc_and_year_to_vals[(cm_count, data["year"])][key].append(v_)
                                    else:
                                        cmc_and_year_to_vals[(cm_count, data["year"])][key].append(v)
                                # if a key is missing, because that monica event was never raised/reached, create the empty list
                                # so a no-data value is being produced
                                else:
                                    cmc_and_year_to_vals[(cm_count, data["year"])][key]

                    # potentially aggregate multiple data per cell and finally store them for this row
                    for (cm_count, year), key_to_vals in cmc_and_year_to_vals.items():
                        for key, vals in key_to_vals.items():
                            output_vals = output_grids[key]["data"]
                            if len(vals) > 0:
                                output_vals[(cm_count, year)][col - col_0] = sum(vals) / len(vals)
                            else:
                                output_vals[(cm_count, year)][col - col_0] = -9999

        is_no_data_row = no_data_cols == no_of_cols

    if is_no_data_row:
        write_row_to_grids.nodata_row_count[setup_id] += 1

    def write_nodata_rows(file_):
        for _ in range(write_row_to_grids.nodata_row_count[setup_id]):
            rowstr = " ".join(["-9999" for __ in range(no_of_cols)])
            file_.write(rowstr + "\n")

    # iterate over all prepared data for a single row and write row
    for key, y2d_ in output_grids.items():
        y2d = y2d_["data"]
        cast_to = y2d_["cast-to"]
        digits = y2d_.get("digits", 0)
        if cast_to == "int":
            mold = lambda x: str(int(x))
        else:
            mold = lambda x: str(round(x, digits))

        for (cm_count, year), row_arr in y2d.items():
            crop = cmc_to_crop[cm_count] if cm_count in cmc_to_crop else "none"
            crop = crop.replace("/", "").replace(" ", "")
            path_to_file = path_to_output_dir + crop + "_" + key + "_" + str(year) + "_" + str(cm_count) + ".asc"

            if not os.path.isfile(path_to_file):
                with open(path_to_file, "w") as _:
                    _.write(header)
                    write_row_to_grids.list_of_output_files[setup_id].append(path_to_file)

            with open(path_to_file, "a") as file_:
                write_nodata_rows(file_)
                rowstr = " ".join(["-9999" if int(x) == -9999 else mold(x) for x in row_arr])
                file_.write(rowstr + "\n")

    # clear the no-data row count when no-data rows have been written before a data row
    if not is_no_data_row:
        write_row_to_grids.nodata_row_count[setup_id] = 0

    # if we're at the end of the output and just empty lines are left, then they won't be written in the
    # above manner because there won't be any rows with data where they could be written before
    # so add no-data rows simply to all files we've written to before
    if is_no_data_row \
            and write_row_to_grids.list_of_output_files[setup_id] \
            and write_row_to_grids.nodata_row_count[setup_id] > 0:
        for path_to_file in write_row_to_grids.list_of_output_files[setup_id]:
            with open(path_to_file, "a") as file_:
                write_nodata_rows(file_)
        write_row_to_grids.nodata_row_count[setup_id] = 0

    if row in row_col_data:
        del row_col_data[row]


def run_consumer(leave_after_finished_run=True, server={"server": None, "port": None}):
    """collect data from workers"""

    config = {
        "mode": "mbm-local-remote",
        "port": server["port"] if server["port"] else "7777",  # local 7778,  remote 7777
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "timeout": 600000  # 10 minutes
    }

    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    paths = PATHS[config["mode"]]

    if not "out" in config:
        config["out"] = paths["path-to-output-dir"]
    if not "csv-out" in config:
        config["csv-out"] = paths["path-to-csv-output-dir"]

    context = zmq.Context()
    socket = context.socket(zmq.PULL)

    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    socket.RCVTIMEO = config["timeout"]
    leave = False

    setup_id_to_data = defaultdict(lambda: {
        "header": None,
        "no_of_cols": None,
        "no_of_rows": None,
        "out_dir_exists": False,
        "row_col_data": defaultdict(lambda: defaultdict(list)),
        "cols@row_received": {},
        "next_row": None
    })


    def process_message(msg):
        if len(msg["errors"]) > 0:
            print("There were errors in message:", msg, "\nSkipping message!")
            return

        if not hasattr(process_message, "wnof_count"):
            process_message.wnof_count = 0
            process_message.setup_count = 0

        leave = False

        custom_id = msg["customId"]
        setup_id = custom_id["setup_id"]
        region = custom_id["region"]
        planting = custom_id["planting"]
        nitrogen = custom_id["nitrogen"]
        crop = custom_id["crop"]

        data = setup_id_to_data[setup_id]

        row = custom_id["s_row"]
        col = custom_id["s_col"]
        no_of_cols = custom_id["no_of_s_cols"]
        no_of_rows = custom_id["no_of_s_rows"]
        row_0 = custom_id["s_row_0"]
        col_0 = custom_id["s_col_0"]
        if row not in data["cols@row_received"]:
            data["cols@row_received"][row] = 0
        if data["next_row"] is None:
            data["next_row"] = row_0
        if data["header"] is None:
            data["header"] = f"""ncols        {no_of_cols}
nrows        {no_of_rows}
xllcorner    {custom_id["b_lon_0"]}
yllcorner    {custom_id["b_lat_0"] - (no_of_rows * custom_id["s_resolution"])}
cellsize     {custom_id["s_resolution"]}
NODATA_value -9999
"""
        is_nodata = custom_id["nodata"]

        debug_msg = "received work result " + str(process_message.received_env_count) \
                    + " customId: " + str(msg.get("customId", "")) \
                    + " next row: " + str(data["next_row"]) \
                    + " cols@row to go: " + str(no_of_cols - data["cols@row_received"][row]) + "@" \
                    + str(row) + " cols_per_row: " + str(no_of_cols)
        print(debug_msg)
        # debug_file.write(debug_msg + "\n")
        if is_nodata:
            data["row_col_data"][row][col] = -9999
        else:
            data["row_col_data"][row][col].append(calculate_index_data(msg))
        data["cols@row_received"][row] += 1

        #process_message.received_env_count = process_message.received_env_count + 1

        while (data["next_row"] in data["row_col_data"] and
               data["cols@row_received"][data["next_row"]] == no_of_cols):   #\
                #or (len(data["cols@row_received"]) > data["next_row"] and
                #    data["cols@row_received"][data["next_row"]] == 0):

            path_to_out_dir = f"{config['out']}{setup_id}_reg-{region}_{crop}_plant-{planting}_{nitrogen}-N/"
            print(path_to_out_dir)
            if not data["out_dir_exists"]:
                if os.path.isdir(path_to_out_dir) and os.path.exists(path_to_out_dir):
                    data["out_dir_exists"] = True
                else:
                    try:
                        os.makedirs(path_to_out_dir)
                        data["out_dir_exists"] = True
                    except OSError:
                        print("c: Couldn't create dir:", path_to_out_dir, "! Exiting.")
                        exit(1)

            write_row_to_grids(data["row_col_data"], data["next_row"], col_0, no_of_cols, data["header"],
                               path_to_out_dir, setup_id)

            debug_msg = "wrote row: " + str(data["next_row"]) \
                        + " next_row: " + str(data["next_row"] + 1) \
                        + " rows unwritten: " + str(list(data["row_col_data"].keys()))
            print(debug_msg)
            # debug_file.write(debug_msg + "\n")

            data["next_row"] += 1  # move to next row (to be written)

            # this setup is finished
            if leave_after_finished_run and data["next_row"] > (row_0 + no_of_rows):
                process_message.setup_count += 1

        process_message.received_env_count = process_message.received_env_count + 1
        return leave

    process_message.received_env_count = 1

    while not leave:
        try:
            # start_time_recv = timeit.default_timer()
            msg = socket.recv_json()  # encoding="latin-1"
            # elapsed = timeit.default_timer() - start_time_recv
            # print("time to receive message" + str(elapsed))
            # start_time_proc = timeit.default_timer()
            leave = process_message(msg)
            # elapsed = timeit.default_timer() - start_time_proc
            # print("time to process message" + str(elapsed))
        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            return
        except Exception as e:
            print("Exception:", e)
            # continue

    print("exiting run_consumer()")
    # debug_file.close()


if __name__ == "__main__":
    run_consumer()
