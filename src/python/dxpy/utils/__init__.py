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

"""
Utilities shared by dxpy modules.
"""

from __future__ import print_function, unicode_literals, division, absolute_import

import os, json, collections, concurrent.futures, traceback, sys, time, gc
from multiprocessing import cpu_count
import dateutil.parser
from .. import logger
from ..compat import basestring, THREAD_TIMEOUT_MAX, Mapping
from ..exceptions import DXError
import numbers
import binascii
import random

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
            future = next(concurrent.futures.as_completed(futures, timeout=THREAD_TIMEOUT_MAX))
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


def response_iterator(request_iterator, thread_pool, max_active_tasks=None, do_first_task_sequentially=True):
    """
    :param request_iterator:
        An iterator producing inputs for consumption by the worker pool.
    :type request_iterator: iterator of callable, args, kwargs
    :param thread_pool: thread pool to submit the requests to
    :type thread_pool: concurrent.futures.thread.ThreadPoolExecutor
    :param max_active_tasks:
        The maximum number of tasks that may be either running or
        waiting for consumption of their result. If not given, defaults
        to the number of CPU cores on the machine.
    :type max_active_tasks: int
    :param do_first_task_sequentially:
        If True, executes (and returns the result of) the first request
        before submitting any other requests (the subsequent requests
        are submitted with *max_active_tasks* parallelism).
    :type do_first_task_sequentially: bool

    Rate-limited asynchronous multithreaded task runner. Consumes tasks
    from *request_iterator*. Yields their results in order, while
    allowing up to *max_active_tasks* to run simultaneously. Unlike
    concurrent.futures.Executor.map, prevents new tasks from starting
    while there are *max_active_tasks* or more unconsumed results.

    """
    tasks_in_progress = collections.deque()
    if max_active_tasks is None:
        max_active_tasks = cpu_count()

    # The following two functions facilitate GC by not adding extra variables to the enclosing scope.
    def submit_task(task_iterator, executor, futures_queue):
        retval  = next(task_iterator, None)
        if retval is None:
            return False
        task_callable, task_args, task_kwargs = retval
        task_future = executor.submit(task_callable, *task_args, **task_kwargs)
        futures_queue.append(task_future)
        return True

    def next_result(tasks_in_progress):
        future = tasks_in_progress.popleft()
        try:
            result = future.result(timeout=THREAD_TIMEOUT_MAX)
        except KeyboardInterrupt:
            print('')
            os._exit(os.EX_IOERR)
        return result

    if do_first_task_sequentially:
        task_callable, task_args, task_kwargs = next(request_iterator)
        yield task_callable(*task_args, **task_kwargs)

    for _i in range(max_active_tasks):
        retval = submit_task(request_iterator, thread_pool, tasks_in_progress)
        if not retval:
            break

    while len(tasks_in_progress) > 0:
        result = next_result(tasks_in_progress)
        submit_task(request_iterator, thread_pool, tasks_in_progress)
        yield result
        del result

def string_buffer_length(buf):
    orig_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(orig_pos)
    return buf_len


def normalize_time_input(t, future=False, default_unit='ms'):
    """
    :param default_unit: units of the input time *t*; must be one of "s" or
        "ms". This param is only respected if *t* looks like an int (e.g.
        "12345", 12345).
    :type default_unit: string

    Converts inputs such as:
       "2012-05-01"
       "-5d"
       1352863174
       "1352863174"
    to milliseconds since epoch. See http://labix.org/python-dateutil and :meth:`normalize_timedelta`.
    """
    error_msg = 'Error: Expected an int timestamp, a date format (e.g. YYYY-MM-DD), or an int with a single-letter suffix (s=seconds, m=minutes, h=hours, d=days, w=weeks, M=months, y=years; e.g. "-10d" indicates 10 days ago); but got {t}'
    if isinstance(t, basestring) and t.isdigit():
        t = int(t)

    if isinstance(t, basestring):
        try:
            t = normalize_timedelta(t)
        except ValueError:
            try:
                t = int(time.mktime(dateutil.parser.parse(t).timetuple())*1000)
                assert t > 0
            except (ValueError, OverflowError, AssertionError):
                raise ValueError(error_msg.format(t=t))
    elif isinstance(t, numbers.Integral):
        units_multipliers = {'ms': 1, 's': 1000}
        if default_unit not in units_multipliers:
            raise ValueError("Expected default_unit to be one of 's' or 'ms'")
        t = t * units_multipliers[default_unit]
    else:
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
        if isinstance(v, Mapping):
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

class Nonce:
    '''
    Generates a nonce by using the system's random number generator. If it fails
    it uses python's random library to generate a random long integer.
    The nonce is the random number concatenated with the time.
    '''
    def __init__(self):
        try:
            self.nonce = "%s%f" % (str(binascii.hexlify(os.urandom(32))), time.time())
        except:
            random.seed(time.time())
            self.nonce = "%s%f" % (str(random.getrandbits(8*26)), time.time())

    def __str__(self):
        return self.nonce

    @staticmethod
    def update_nonce(input_params):
        '''
        Static method to return a copy of the input dictionary with an
        additional unique nonce
        :param input: an input dictionary that may be empty
        :type input: dict
        :returns an extended copy of the input with an additional nonce field

        The input dictionary is updated with a nonce only if does not already
        have a non empty nonce
        '''
        input_cp = input_params.copy()
        if len(input_cp.get('nonce', '')) == 0:
            input_cp['nonce'] = str(Nonce())
        return input_cp

# Moved to the bottom due to circular imports
from .exec_utils import run, convert_handlers_to_dxlinks, parse_args_as_job_input, entry_point, DXJSONEncoder
