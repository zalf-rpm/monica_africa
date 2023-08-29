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
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from pkgs.common import common

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
common_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "common.capnp"), imports=abs_imports)
geo_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "geo.capnp"), imports=abs_imports)

config = {
    "in_sr": None,  # some value struct
    "out_srs": "",  # sturdy refs separated by |
}
common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

debug_out = config["debug_out"] == "true"

conman = common.ConnectionManager()
in_p = conman.try_connect(config["in_sr"], cast_as=common_capnp.Channel.Reader, retry_secs=1)
out_srs = config["out_srs"].split("|")
out_ps = []
for out_sr in out_srs:
    out_ps.append(conman.try_connect(config["out_sr"], cast_as=common_capnp.Channel.Writer, retry_secs=1))

try:
    if in_p and any(out_ps):
        while True:
            msg = in_p.read().wait()
            # check for end of data from in port
            if msg.which() == "done":
                break
            
            in_ip = msg.value.as_struct(common_capnp.IP)
            for out_p in out_ps:
                if out_p:
                    c = in_ip.content.copy()
                    out_ip = common_capnp.IP.new_message(content=c)
                    common.copy_and_set_fbp_attrs(in_ip, out_ip)
                    out_p.write(value=out_ip).wait()

        # close out ports
        for out_p in out_ps:
            if out_p:
                out_p.write(done=None).wait()

except Exception as e:
    print("copy_value.py ex:", e)

print("copy_value.py: exiting run")

