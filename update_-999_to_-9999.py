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

import in_place
import os
import sys

def in_place_update_files(dir, files):
    files.sort()
    for file in files:
        #print("file:", file)
        #continue
        if file.endswith(".asc"):
            with in_place.InPlace(os.path.join(dir, file)) as fp:
                for i, line in enumerate(fp):
                    if i == 5:
                        line = line.replace("-999", "-9999")
                    fp.write(line) 

def copy_update_files(from_dir, files, to_dir):
    #print("copy files from:", from_dir, "to dir:", to_dir)
    files.sort()
    for file in files:
        #print("file:", os.path.join(from_dir, file), " to:", os.path.join(to_dir, file))
        #continue
        if file.endswith(".asc"):
            with open(os.path.join(from_dir, file), mode="r") as fp:
                with open(os.path.join(to_dir, file), mode="w") as tp:
                    for i, line in enumerate(fp):
                        if i == 5:
                            line = line.replace("-999", "-9999")
                        tp.write(line) 

if len(sys.argv) > 2:
    from_path = sys.argv[1]
    to_path = sys.argv[2]
    from_setup = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    to_setup = int(sys.argv[4]) if len(sys.argv) > 4 else 136
    for root1, dirs1, files1 in os.walk(from_path):
        if len(dirs1) > 0:
            dirs1.sort()
            for dir1 in dirs1:
                if from_setup <= int(dir1) and int(dir1) <= to_setup:
                    os.makedirs(os.path.join(to_path, dir1), exist_ok=True)
                    for root2, dirs2, files2 in os.walk(os.path.join(root1, dir1)):
                        #in_place_update_files(root, files)
                        copy_update_files(root2, files2, os.path.join(to_path, dir1))
        if len(files1) > 0:
            #in_place_update_files(files)
            copy_update_files(root1, files1, to_path)