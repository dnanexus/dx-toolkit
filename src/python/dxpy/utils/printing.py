'''
This submodule gives basic utilities for printing to the terminal.
'''

import textwrap, subprocess, sys

if sys.stdout.isatty():
    try:
        p = subprocess.Popen(['stty', 'size'],
                             stdin=sys.stdout,
                             stdout=subprocess.PIPE)
        tty_rows, tty_cols = map(int, p.stdout.read().split())
        std_width = min(tty_cols - 2, 100)
    except:
        tty_rows, tty_cols = 24, 80
        std_width = 78
    color_state = True
else:
    tty_rows, tty_cols = 24, 80
    std_width = 78
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
        kwargs['width'] = max(std_width + width_adjustment, 20)
    if "break_on_hyphens" not in kwargs:
        kwargs["break_on_hyphens"] = False
    return textwrap.fill(string, **kwargs)
