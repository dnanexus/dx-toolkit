# Copyright (C) 2013-2016 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

import itertools
import sys
import time
import threading

def run_with_loader(task, text="Loading...", *args, **kwargs):
    done = threading.Event()
    def spinner():
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if done.is_set():
                break
            sys.stdout.write(f'\r{text} ' + c)
            sys.stdout.flush()
            time.sleep(0.1)
        # Clear loader line
        sys.stdout.write('\r' + ' ' * (len(text) + 2) + '\r')
        sys.stdout.flush()

    t = threading.Thread(target=spinner)
    t.start()
    try:
        result = task(*args, **kwargs)
    finally:
        done.set()
        t.join()

    return result


