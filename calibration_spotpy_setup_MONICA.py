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
import json
import numpy as np
import os
from pathlib import Path
import spotpy
import calibration_MONICA_adapter
import re

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)

class spot_setup(object):
    def __init__(self, user_params, obs_list, prod_writer, cons_reader):
        self.user_params = user_params
        self.params = []
        self.obs_list = obs_list
        self.prod_writer = prod_writer
        self.cons_reader = cons_reader
        for par in user_params:
            par_name = par["name"]
            if "array" in par:
                if re.search(r'\d', par["array"]):  # check if par["array"] contains numbers
                    par_name += "_" + par["array"]  # spotpy does not allow two parameters to have the same name
            if "derive_function" not in par:  # spotpy does not care about derived params
                self.params.append(spotpy.parameter.Uniform(**par))

    def parameters(self):
        return spotpy.parameter.generate(self.params)

    def simulation(self, vector):
        # vector = MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate
        out_ip = fbp_capnp.IP.new_message(content=json.dumps(dict(zip(vector.name, vector))))
        self.prod_writer.write(value=out_ip).wait()
        #print("sent params to monica setup:", vector)

        msg = self.cons_reader.read().wait()
        # check for end of data from in port
        if msg.which() == "done":
            return

        in_ip = msg.value.as_struct(fbp_capnp.IP)
        s: str = in_ip.content.as_text()
        list_of_country_id_and_year_and_avg_yield = json.loads(s)
        list_of_country_id_and_year_and_avg_yield.sort(key=lambda r: [r["id"], r["year"]])
        #print("received monica results:", list_of_country_id_and_year_and_avg_yield)
        sim_list = list(map(lambda d: d["value"], list_of_country_id_and_year_and_avg_yield))
        # besides the order the length of observation results and simulation results should be the same
        assert(len(sim_list) == len(self.obs_list))
        return sim_list

    def evaluation(self):
        return self.obs_list

    def objectivefunction(self, simulation, evaluation):
        objectivefunction = spotpy.objectivefunctions.rmse(evaluation, simulation)
        return objectivefunction
