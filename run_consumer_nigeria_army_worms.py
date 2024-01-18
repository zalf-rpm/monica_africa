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

histogram_keys = [
    "days_in_breeding_window",
    "dry",
    "dry_and_hot",
    "dry_and_cold",
    "wet",
    "wet_and_hot",
    "wet_and_cold",
    "cold",
    "hot",
]


def calculate_index_data(data_sections, aer):
    cm_count_to_season_info = defaultdict(lambda: {
        "year": None,
        "sowing_doy": 0,
        "harvest_doy": 0,
    })
    year_to_worm_index_info = defaultdict(lambda: {
        "crop": "none",
        "worm_index": 0,
        "window_count": 0,
    })
    cm_count_to_worm_index_info = defaultdict(lambda: {
        "worm_index": 0,
        "window_count": 0,
    })
    year_to_stresses = defaultdict(lambda: {
        "crop": "none",
        "dry": 0,
        "dry_and_hot": 0,
        "dry_and_cold": 0,
        "wet": 0,
        "wet_and_hot": 0,
        "wet_and_cold": 0,
        "cold": 0,
        "hot": 0,
    })
    cm_count_to_stresses = defaultdict(lambda: {
        "dry": 0,
        "dry_and_hot": 0,
        "dry_and_cold": 0,
        "wet": 0,
        "wet_and_hot": 0,
        "wet_and_cold": 0,
        "cold": 0,
        "hot": 0,
    })

    aer_to_year_to_week_to_histogram_data = defaultdict(  # aer
        lambda: defaultdict(  # year
            lambda: defaultdict(  # week
                lambda: {k: 0 for k in histogram_keys}
            )
        )
    )

    def check_and_record_if_in_breeding_conditions_window(store, hist_data, days_in_window):
        # during the whole year record the worm index and count the number of windows
        # so if we have 7 days in a row, we add 1 to the worm index
        # and increase the window count by 1
        if days_in_window == 7:
            store["worm_index"] += 1
            store["window_count"] += 1
            hist_data["days_in_breeding_window"] += 1
        # for every further day in the same window increase the index by 1/7
        elif days_in_window > 7:
            store["worm_index"] += 1./7.
            hist_data["days_in_breeding_window"] += 1

    def check_and_record_stresses(store, hist_data, dry_, wet_, cold_, hot_):
        if dry_:
            store["dry"] += 1
            hist_data["dry"] += 1
            if hot_:
                store["dry_and_hot"] += 1
                hist_data["dry_and_hot"] += 1
            if cold_:
                store["dry_and_cold"] += 1
                hist_data["dry_and_cold"] += 1
        if wet_:
            store["wet"] += 1
            hist_data["wet"] += 1
            if hot_:
                store["wet_and_hot"] += 1
                hist_data["wet_and_hot"] += 1
            if cold_:
                store["wet_and_cold"] += 1
                hist_data["wet_and_cold"] += 1
        if hot_:
            store["hot"] += 1
            hist_data["hot"] += 1
        if cold_:
            store["cold"] += 1
            hist_data["cold"] += 1

    for data in data_sections:
        results = data.get("results", [])

        is_daily_section = data.get("origSpec", "") == '"daily"'
        is_crop_section = data.get("origSpec", "") == '"crop"'

        days_in_window = 0
        for vals in results:
            if "CM-count" not in vals or "year" not in vals:
                continue

            if is_crop_section:
                cm_count_to_season_info[vals["CM-count"]]["year"] = vals["year"]
                cm_count_to_season_info[vals["CM-count"]]["sowing_doy"] = vals["sowing_doy"]
                cm_count_to_season_info[vals["CM-count"]]["harvest_doy"] = vals["harvest_doy"]
            elif is_daily_section:
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

                # get week number from doy
                week = datetime.strptime(f"{year} {doy}", "%Y %j").isocalendar()[1]
                week_year = year - (1 if doy < 8 and week > 50 else 0)

                histogram_data = aer_to_year_to_week_to_histogram_data[aer][week_year][week]

                # breeding condition met (Growth Index (GI))
                if 0.15 <= sm <= 0.25 and 16 <= tmax <= 36:
                    # add one day to the window count
                    days_in_window += 1

                    year_to_worm_index_info[year].setdefault("year", year)
                    check_and_record_if_in_breeding_conditions_window(year_to_worm_index_info[year],
                                                                      histogram_data,
                                                                      days_in_window)

                    # additionally record the worm index and count the number of windows
                    # only for the cropping season, so when day of year is between sowing and harvest
                    if s_doy <= doy <= h_doy:
                        cm_count_to_worm_index_info[cmc].setdefault("year", year)
                        cm_count_to_worm_index_info[cmc].setdefault("crop", crop)
                        check_and_record_if_in_breeding_conditions_window(cm_count_to_worm_index_info[cmc],
                                                                          defaultdict(int),
                                                                          days_in_window)

                # stress conditions might apply - original
                 #else:
                     #days_in_window = 0

                     #dry = sm < 0.15
                     #wet = sm > 0.2
                     #cold = tmin < 15
                     #hot = tmax > 36
                # stress conditions might apply - modified
                else:
                    days_in_window = 0

                    dry = sm < 0.15
                    wet = sm > 0.251
                    cold = tmin < 15
                    hot = tmax > 351

                    # during the whole year record the stresses
                    year_to_stresses[year].setdefault("year", vals["year"])
                    check_and_record_stresses(year_to_stresses[year], histogram_data, dry, wet, cold, hot)

                    # and record the same stresses just in the cropping season
                    if s_doy <= doy <= h_doy:
                        cm_count_to_stresses[cmc].setdefault("year", year)
                        cm_count_to_stresses[cmc].setdefault("crop", crop)
                        check_and_record_stresses(cm_count_to_stresses[cmc], defaultdict(int), dry, wet, cold, hot)

    cm_count_to_vals = defaultdict(dict)
    for year, wii in year_to_worm_index_info.items():
        cm_count_to_vals[year] = wii
    for cmc, wii in cm_count_to_worm_index_info.items():
        if cmc in cm_count_to_vals:
            cm_count_to_vals[cmc].update(wii)
        else:
            cm_count_to_vals[cmc] = wii
    for year, s in year_to_stresses.items():
        if year in cm_count_to_vals:
            cm_count_to_vals[year].update(s)
        else:
            cm_count_to_vals[year] = s
    for cmc, s in cm_count_to_stresses.items():
        if cmc in cm_count_to_vals:
            cm_count_to_vals[cmc].update(s)
        else:
            cm_count_to_vals[cmc] = s

    # remove cm_count=0, which we don't need
    cm_count_to_vals.pop(0, None)

    return cm_count_to_vals, aer_to_year_to_week_to_histogram_data


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
        "timeout": 600000*3  # 30 minutes
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
        "next_row": None,
        "no_of_envs_expected": None,
        "envs_received": 0,
    })

    aer_to_year_to_week_to_histogram_data = defaultdict(  # aer
        lambda: defaultdict(  # year
            lambda: defaultdict(  # week
                lambda: {k: [] for k in histogram_keys}
            )
        )
    )
    cached_hist_data = []

    while True:
        try:
            msg = socket.recv_json()  # encoding="latin-1"

            if len(msg["errors"]) > 0:
                print("There were errors in message:", msg, "\nSkipping message!")
                continue

            custom_id = msg["customId"]
            setup_id = custom_id["setup_id"]
            data = setup_id_to_data[setup_id]

            if "no_of_sent_envs" in custom_id:
                data["no_of_envs_expected"] = custom_id["no_of_sent_envs"]
            else:
                region = custom_id["region"]
                planting = custom_id["planting"]
                nitrogen = custom_id["nitrogen"]
                crop = custom_id["crop"]
                aer = custom_id["aer"]

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
                    # noinspection PyTypeChecker
                    data["header"] = \
                        f"ncols        {no_of_cols}\n" + \
                        f"nrows        {no_of_rows}\n" + \
                        f"xllcorner    {custom_id['b_lon_0']}\n" + \
                        f"yllcorner    {custom_id['b_lat_0'] - (no_of_rows * custom_id['s_resolution'])}\n" + \
                        f"cellsize     {custom_id['s_resolution']}\n" + \
                        f"NODATA_value -9999\n"

                is_nodata = custom_id["nodata"]

                debug_msg = f"received work result {data['envs_received']} " + \
                            f"customId: {msg.get('customId', '')} " + \
                            f"next row: {data['next_row']} " + \
                            f"cols@row to go: {no_of_cols - data['cols@row_received'][row]}@{row} " + \
                            f"cols_per_row: {no_of_cols}"
                print(debug_msg)
                # debug_file.write(debug_msg + "\n")
                if is_nodata:
                    data["row_col_data"][row][col] = -9999
                else:
                    grid_data, histogram_data = calculate_index_data(msg.get("data", []), aer)
                    cached_hist_data.append(histogram_data)
                    data["row_col_data"][row][col].append(grid_data)
                data["cols@row_received"][row] += 1

                data["envs_received"] += 1

                while (data["next_row"] in data["row_col_data"] and
                       data["cols@row_received"][data["next_row"]] == no_of_cols):

                    path_to_out_dir = f"{config['out']}{setup_id}_reg-{region}_{crop}_plant-{planting}_{nitrogen}-N/"
                    print(path_to_out_dir)
                    if not os.path.exists(path_to_out_dir):
                        try:
                            os.makedirs(path_to_out_dir)
                        except OSError:
                            print("c: Couldn't create dir:", path_to_out_dir, "! Exiting.")
                            exit(1)

                    write_row_to_grids(data["row_col_data"], data["next_row"], col_0, no_of_cols,
                                       data["header"], path_to_out_dir, setup_id)

                    debug_msg = "wrote row: " + str(data["next_row"]) \
                                + " next_row: " + str(data["next_row"] + 1) \
                                + " rows unwritten: " + str(list(data["row_col_data"].keys()))
                    print(debug_msg)

                    data["next_row"] += 1  # move to next row (to be written)

            # this setup is finished
            if data["no_of_envs_expected"] and data["no_of_envs_expected"] == data["envs_received"]:

                # write histogram csv files
                print("writing histogram csv files")

                # transform cached data
                for hist_data in cached_hist_data:
                    for aer, d in hist_data.items():
                        for y, d2 in d.items():
                            for w, d3 in d2.items():
                                for k, v in d3.items():
                                    aer_to_year_to_week_to_histogram_data[aer][y][w][k].append(v)

                path_to_csv_out_dir = f"{config['csv-out']}"
                print(path_to_csv_out_dir)
                if not os.path.exists(path_to_csv_out_dir):
                    try:
                        os.makedirs(path_to_csv_out_dir)
                    except OSError:
                        print("c: Couldn't create dir:", path_to_csv_out_dir, "! Exiting.")
                        exit(1)

                for aer, year_to_week_to_hist_data in aer_to_year_to_week_to_histogram_data.items():
                    path_to_csv_file = f"{path_to_csv_out_dir}{setup_id}_aer-{aer}.csv"
                    with open(path_to_csv_file, "w") as csv_file:
                        writer = csv.writer(csv_file, delimiter=",")
                        writer.writerow(["year", "week"] +
                                        [f"{k}|sum|1-week" for k in histogram_keys] +
                                        [f"{k}|avg|7-days" for k in histogram_keys] +
                                        [f"{k}|sum|7-days" for k in histogram_keys])
                        for year, week_to_hist_data in year_to_week_to_hist_data.items():
                            for week, hist_data in week_to_hist_data.items():
                                writer.writerow([year, week] +
                                                [sum(map(lambda c: 1 if c > 0 else 0, hist_data[k]))
                                                 for k in histogram_keys] +
                                                [sum(hist_data[k])/len(hist_data[k]) for k in histogram_keys] +
                                                [sum(hist_data[k]) for k in histogram_keys])

                # remove setup
                del setup_id_to_data[setup_id]
                if len(setup_id_to_data) == 0:
                    break

        except zmq.error.Again as _e:
            print('no response from the server (with "timeout"=%d ms) ' % socket.RCVTIMEO)
            return
        except Exception as e:
            print("Exception:", e)

    print("exiting run_consumer()")


if __name__ == "__main__":
    run_consumer()
