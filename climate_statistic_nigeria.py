
import sys
import os
import monica_run_lib
import numpy as np
import shared
import gzip

config = {
    "climate_data_path": "/beegfs/common/data/climate",
    "gcm": ["GFDL-ESM4","UKESM1-0-LL"],
    "scenario": ["historical","ssp126"],
    "ensmem": "r1i1p1f1",
    "start-year": 1970, 
    "end-year": 2020,
    "column_name": "tavg",
    "climate_zone_lookup": "./data/agro_ecological_regions_nigeria/early_planting_high_nitrogen.csv",
    "climate_zone_config": "./data/agro_ecological_regions_nigeria/agro-eco-regions_0.038deg_4326_wgs84_nigeria.asc",
    "resolution": "5min"
}

TEMP_CLIMATE_FILE = "isimip/3b_v1.1_CMIP6/csvs/{gcm}/{scenario}/{ensmem}/row-{crow}/col-{ccol}.csv.gz"
TEMP_OUTPUT_FILE = "climate_statistic_nigeria_{gcm}.csv"

def run_stats():
    """
    Run the climate statistic program.
    """
    # takes as input:
    # - climate data path 
    # - list of gcm
    # - scenario (combine historical and ssp126)
    # - ensmem
    # - start year
    # - end year
    # - column name of the column to calculate the average of
    # - climate zone lookup file (with index and climate zone name)
    # - configuation file to map the climate zones
    # - resolution of the lat lon grid

    # returns:
    # - for each climate zone:
    #   an average value per year of the given column 
    #   (e.g. average temperature per year)
    # columns: years
    # rows: climate zones

    # parse arguments
    shared.update_config(config, sys.argv, print_config=True, allow_new_keys=False)
    s_resolution = {"5min": 5 / 60., "30sec": 30 / 3600.}[config["resolution"]]
    s_res_scale_factor = {"5min": 60., "30sec": 3600.}[config["resolution"]]

    # read climate zone lookup
    climateZoneLookup = dict()
    with open(config["climate_zone_lookup"]) as f:
        for line in f:
            # skip first line
            if line.startswith("id"):
                continue
            token = line.split(",")
            climateZoneIndex = token[0].strip()
            climateZone = token[1].strip()
            climateZoneLookup[climateZoneIndex] = climateZone
    # read climate zone config
    path_to_eco_grid = config["climate_zone_config"]
    eco_metadata, _ = monica_run_lib.read_header(path_to_eco_grid)
    eco_grid = np.loadtxt(path_to_eco_grid, dtype=int, skiprows=len(eco_metadata))
    aer_ll0r = shared.get_lat_0_lon_0_resolution_from_grid_metadata(eco_metadata)

    #nigeria lat lon bounds
    lat_lon_bounds = {"tl": {"lat": 14.0, "lon": 2.7}, "br": {"lat": 4.25, "lon": 14.7}}
    c_lon_0 = -179.75
    c_lat_0 = +89.25
    c_resolution = 0.5

    lats_scaled = range(int(lat_lon_bounds["tl"]["lat"] * s_res_scale_factor),
                        int(lat_lon_bounds["br"]["lat"] * s_res_scale_factor) - 1,
                        -int(s_resolution * s_res_scale_factor))
    lons_scaled = range(int(lat_lon_bounds["tl"]["lon"] * s_res_scale_factor),
                int(lat_lon_bounds["br"]["lon"] * s_res_scale_factor) + 1,
                int(s_resolution * s_res_scale_factor))
    
    for gcm in config["gcm"]:

        numValuesClimateZonePerYear = dict()
        for climateZone in climateZoneLookup:
            numValuesClimateZonePerYear[climateZone] = dict()
            for year in range(config["start-year"], config["end-year"] + 1):
                numValuesClimateZonePerYear[climateZone][year] = 0

        sumValuesClimateZonePerYear = dict()
        for climateZone in climateZoneLookup:
            sumValuesClimateZonePerYear[climateZone] = dict()
            for year in range(config["start-year"], config["end-year"] + 1):
                sumValuesClimateZonePerYear[climateZone][year] = 0

        for lat_scaled in lats_scaled:
            lat = lat_scaled / s_res_scale_factor

            for lon_scaled in lons_scaled:
                lon = lon_scaled / s_res_scale_factor

                aer_col = int((lon - aer_ll0r["lon_0"]) / aer_ll0r["res"])
                aer_row = int((aer_ll0r["lat_0"] - lat) / aer_ll0r["res"])
                currentClimateZoneIndex = None
                if 0 <= aer_row < int(eco_metadata["nrows"]) and 0 <= aer_col < int(eco_metadata["ncols"]):
                    aer = eco_grid[aer_row, aer_col]
                    if aer > 0 :
                        aer = str(aer)
                        if aer in climateZoneLookup:
                            currentClimateZoneIndex = aer

                if currentClimateZoneIndex is not None:
                    c_col = int((lon - c_lon_0) / c_resolution)
                    c_row = int((c_lat_0 - lat) / c_resolution)

                    for scenario in config["scenario"]:

                        climFile = TEMP_CLIMATE_FILE.format(gcm=gcm, scenario=scenario, ensmem=config["ensmem"], crow=c_row, ccol=c_col)
                        climateFilePath = os.path.join(config["climate_data_path"], climFile)

                        firstline = True
                        secondline = True
                        columnIndex = -1

                        # read zipped climate file

                        with gzip.open(climateFilePath,'rt') as f:
                            for line in f:
                                
                                if firstline:
                                    firstline = False
                                    # get column names in first line
                                    columnNames = line.split(",")
                                    # get index of column name
                                    columnIndex = columnNames.index(config["column_name"])
                                    continue
                                if secondline:
                                    secondline = False
                                    continue
                                # get year in first 4 characters of line
                                year = line[:4]
                                year = int(year)
                                if config["start-year"] <= year <= config["end-year"]:
                                    # get value in the column
                                    value = line.split(",")[columnIndex]
                                    # add value to sum
                                    numValuesClimateZonePerYear[currentClimateZoneIndex][year] += 1
                                    sumValuesClimateZonePerYear[currentClimateZoneIndex][year] += float(value)

        # calculate average
        averageValuesClimateZonePerYear = dict()
        for climateZone in climateZoneLookup:
            averageValuesClimateZonePerYear[climateZone] = dict()
            for year in range(config["start-year"], config["end-year"] + 1):
                averageValuesClimateZonePerYear[climateZone][year] = sumValuesClimateZonePerYear[climateZone][year] / numValuesClimateZonePerYear[climateZone][year]

        # write output as csv
        with open(TEMP_OUTPUT_FILE.format(gcm=gcm), "w") as f:
            # column names are years
            # row names are climate zones
            f.write("climate_zone/years")
            for year in range(config["start-year"], config["end-year"] + 1):
                f.write(",{}".format(year))
            f.write("\n")
            for climateZone in climateZoneLookup:
                f.write(climateZoneLookup[climateZone])
                for year in range(config["start-year"], config["end-year"] + 1):
                    f.write(",{}".format(averageValuesClimateZonePerYear[climateZone][year]))
                f.write("\n")



if __name__ == "__main__":
    run_stats()
