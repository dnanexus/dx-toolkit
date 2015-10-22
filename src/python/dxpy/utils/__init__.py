# Copyright (C) 2013-2015 DNAnexus, Inc.
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

"""
Utilities shared by dxpy modules.
"""

from __future__ import (print_function, unicode_literals)

import os, json, collections, concurrent.futures, traceback, sys, time, gc
from multiprocessing import cpu_count
import dateutil.parser
from .. import logger
from ..compat import basestring


def _force_quit(signum, frame):
    # traceback.print_stack(frame)
    os._exit(os.EX_IOERR)

def get_futures_threadpool(max_workers):
    return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

def wait_for_a_future(futures, print_traceback=False):
    """
    Return the next future that completes.  If a KeyboardInterrupt is
    received, then the entire process is exited immediately.  See
    wait_for_all_futures for more notes.
    """
    while True:
        try:
            future = next(concurrent.futures.as_completed(futures, timeout=sys.maxint))
            break
        except concurrent.futures.TimeoutError:
            pass
        except KeyboardInterrupt:
            if print_traceback:
                traceback.print_stack()
            else:
                print('')
            os._exit(os.EX_IOERR)

    return future

def wait_for_all_futures(futures, print_traceback=False):
    """
    Wait indefinitely for all futures in the input iterable to complete.
    Use a timeout to enable interrupt handling.
    Call os._exit() in case of KeyboardInterrupt. Otherwise, the atexit registered handler in concurrent.futures.thread
    will run, and issue blocking join() on all worker threads, requiring us to listen to events in worker threads
    in order to enable timely exit in response to Ctrl-C.

    Note: This still doesn't handle situations where Ctrl-C is pressed elsewhere in the code and there are worker
    threads with long-running tasks.

    Note: os._exit() doesn't work well with interactive mode (e.g. ipython). This may help:
    import __main__ as main; if hasattr(main, '__file__'): os._exit() else: os.exit()
    """
    try:
        while True:
            waited_futures = concurrent.futures.wait(futures, timeout=60)
            if len(waited_futures.not_done) == 0:
                break
    except KeyboardInterrupt:
        if print_traceback:
            traceback.print_stack()
        else:
            print('')
        os._exit(os.EX_IOERR)


def response_iterator(request_iterator, thread_pool, max_active_tasks=None):
    """
    :param request_iterator: An iterator producing inputs for consumption by the worker pool.
    :type request_iterator: iterator of callable, args, kwargs
    :param thread_pool: thread pool to submit the requests to
    :type thread_pool: concurrent.futures.thread.ThreadPoolExecutor
    :param max_active_tasks:
        The maximum number of tasks that may be either running or waiting for consumption of their result.
        If not given, defaults to the number of CPU cores on the machine.
    :type max_active_tasks: int

    Rate-limited asynchronous multithreaded task runner.
    Consumes tasks from *request_iterator*. Yields their results in order, while allowing up to *max_active_tasks* to run
    simultaneously. Unlike concurrent.futures.Executor.map, prevents new tasks from starting while there are
    *max_active_tasks* or more unconsumed results.
    """
    tasks_in_progress = collections.deque()
    if max_active_tasks is None:
        max_active_tasks = cpu_count()

    # The following two functions facilitate GC by not adding extra variables to the enclosing scope.
    def submit_task(task_iterator, executor, futures_queue):
        task_callable, task_args, task_kwargs = next(task_iterator)
        task_future = executor.submit(task_callable, *task_args, **task_kwargs)
        futures_queue.append(task_future)

    def next_result(tasks_in_progress):
        future = tasks_in_progress.popleft()
        try:
            result = future.result(timeout=sys.maxint)
        except KeyboardInterrupt:
            print('')
            os._exit(os.EX_IOERR)
        return result

    for _i in range(max_active_tasks):
        try:
            submit_task(request_iterator, thread_pool, tasks_in_progress)
        except StopIteration:
            break

    while len(tasks_in_progress) > 0:
        result = next_result(tasks_in_progress)

        try:
            submit_task(request_iterator, thread_pool, tasks_in_progress)
        except StopIteration:
            pass

        yield result
        del result

def string_buffer_length(buf):
    orig_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(orig_pos)
    return buf_len

def normalize_time_input(t, future=False):
    """
    Converts inputs such as:
       "2012-05-01"
       "-5d"
       1352863174
    to milliseconds since epoch. See http://labix.org/python-dateutil and :meth:`normalize_timedelta`.
    """
    error_msg = 'Error: Could not parse {t} as a timestamp or timedelta.  Expected a date format or an integer with a single-letter suffix: s=seconds, m=minutes, h=hours, d=days, w=weeks, M=months, y=years, e.g. "-10d" indicates 10 days ago'
    if isinstance(t, basestring):
        try:
            t = normalize_timedelta(t)
        except ValueError:
            try:
                t = int(time.mktime(dateutil.parser.parse(t).timetuple())*1000)
            except ValueError:
                raise ValueError(error_msg.format(t=t))
    now = int(time.time()*1000)
    if t < 0 or (future and t < now):
        t += now
    return t

def normalize_timedelta(timedelta):
    """
    Given a string like "1w" or "-5d", convert it to an integer in milliseconds.
    Integers without a suffix are interpreted as seconds.
    Note: not related to the datetime timedelta class.
    """
    try:
        return int(timedelta) * 1000
    except ValueError as e:
        t, suffix = timedelta[:-1], timedelta[-1:]
        suffix_multipliers = {'s': 1000, 'm': 1000*60, 'h': 1000*60*60, 'd': 1000*60*60*24, 'w': 1000*60*60*24*7,
                              'M': 1000*60*60*24*30, 'y': 1000*60*60*24*365}
        if suffix not in suffix_multipliers:
            raise ValueError()
        return int(t) * suffix_multipliers[suffix]

# See http://stackoverflow.com/questions/4126348
class OrderedDefaultdict(collections.OrderedDict):
    def __init__(self, *args, **kwargs):
        newdefault = None
        newargs = ()
        if args:
            newdefault = args[0]
            if not (newdefault is None or callable(newdefault)):
                raise TypeError('first argument must be callable or None')
            newargs = args[1:]
        self.default_factory = newdefault
        super(self.__class__, self).__init__(*newargs, **kwargs)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        args = self.default_factory if self.default_factory else tuple()
        return type(self), args, None, None, self.items()

def group_array_by_field(array, field='group'):
    groups = OrderedDefaultdict(list)
    for item in array:
        if field not in item and None not in groups:
            groups[None] = []
        groups[item.get(field)].append(item)
    return groups

def merge(d, u):
    """
    Recursively updates a dictionary.
    Example: merge({"a": {"b": 1, "c": 2}}, {"a": {"b": 3}}) = {"a": {"b": 3, "c": 2}}
    """
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = merge(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def _dict_raise_on_duplicates(ordered_pairs):
    """
    Reject duplicate keys.
    """
    d = {}
    for k, v in ordered_pairs:
        if k in d:
           raise ValueError("duplicate key: %r" % (k,))
        else:
           d[k] = v
    return d

def json_load_raise_on_duplicates(*args, **kwargs):
    """
    Like json.load(), but raises an error on duplicate keys.
    """
    kwargs['object_pairs_hook'] = _dict_raise_on_duplicates
    return json.load(*args, **kwargs)

def json_loads_raise_on_duplicates(*args, **kwargs):
    """
    Like json.loads(), but raises an error on duplicate keys.
    """
    kwargs['object_pairs_hook'] = _dict_raise_on_duplicates
    return json.loads(*args, **kwargs)

def warn(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# Moved to the bottom due to circular imports
from .exec_utils import run, convert_handlers_to_dxlinks, parse_args_as_job_input, entry_point, DXJSONEncoder
