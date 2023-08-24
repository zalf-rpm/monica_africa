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
# This file is part of the util library used by models created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import capnp
import os
from pathlib import Path
import sys

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent.parent

PATH_TO_PYTHON_CODE = PATH_TO_REPO / "../mas-infrastructure/src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from common import common as common
from common import geo as geo
import monica_run_lib
import shared

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
geo_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "geo.capnp"), imports=abs_imports)

config = {
    "path_to_grid": None,
    "type": "int",  # int | float
    "debug_out": "true",  # true | false
    "split_at": ",",
    "in_sr": None, # string
    "out_sr": None # utm_coord + id attr
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

debug_out = config["debug_out"] == "true"

conman = common.ConnectionManager()
in_p = conman.try_connect(config["in_sr"], cast_as=common_capnp.Channel.Reader, retry_secs=1)
out_p = conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1)

if not config["path_to_grid"]:
    raise Exception("No path_to_grid given at start of component.")

grid_data = shared.load_grid_cached(config["path_to_grid"], int if config["type"] == "int" else float,
                                    print_path=debug_out)

try:
    if in_p and out_p:
        while True:
            msg = in_p.read().wait()
            # check for end of data from in port
            if msg.which() == "done":
                break
            
            in_ip = msg.value.as_struct(common_capnp.IP)
            ll = in_ip.content.as_struct(geo.name_to_struct_type("latlon"))
            val = grid_data["value"](ll.lat, ll.lon)
            out_ip = common_capnp.IP.new_message(content=str(val))
            common.copy_and_set_fbp_attrs(in_ip, out_ip)
            out_p.write(value=out_ip).wait()

        # close out port
        out_p.write(done=None).wait()

except Exception as e:
    print("get_lat_lon_grid_value.py ex:", e)

print("get_lat_lon_grid_value.py: exiting run")

