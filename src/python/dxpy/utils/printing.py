'''
This submodule gives basic utilities for printing to the terminal.
'''

import os, platform, textwrap

try:
    tty_rows, tty_cols = map(int, os.popen('stty size', 'r').read().split())
    std_width = min(tty_cols - 2, 100)

    color_state = True
except:
    color_state = False

def CYAN():
    return '\033[36m' if color_state else ''

def BLUE():
    return '\033[34m' if color_state else ''

def YELLOW():
    return '\033[33m' if color_state else ''

def GREEN():
    return '\033[32m' if color_state else ''

def RED():
    return '\033[31m' if color_state else ''

def WHITE():
    return '\033[37m' if color_state else ''

def UNDERLINE():
    return '\033[4m' if color_state else ''

def BOLD():
    return '\033[1m' if color_state else ''

def ENDC():
    return '\033[0m' if color_state else ''

def set_colors(state=True):
    global color_state
    color_state = state

def fill(string, width_adjustment=0, **kwargs):
    if "width" not in kwargs:
        kwargs['width'] = std_width + width_adjustment
    if "break_on_hyphens" not in kwargs:
        kwargs["break_on_hyphens"] = False
    return textwrap.fill(string, **kwargs)
