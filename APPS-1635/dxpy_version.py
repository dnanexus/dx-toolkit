#!/usr/bin/env python3

import subprocess

def project_available_instance_types():
    v = subprocess.check_output(
        ["dx", "--version"],
        text=True
    )
    print(v)

if __name__ == "__main__":
    project_available_instance_types()
