# Copyright (C) 2013 DNAnexus, Inc.
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

'''
Utilities shared by dxpy modules.
'''

import os, sys, collections, concurrent.futures, signal, traceback, time, gc
import dateutil.parser
from exec_utils import *


# Monkeypatch ThreadPoolExecutor with relevant logic from the patch for
# Python issue 16284. See:
#
#   <http://bugs.python.org/issue16284>
#   <http://hg.python.org/cpython/rev/70cef0a160cf/>
#
# We may need to apply the relevant parts of the patches to
# ProcessPoolExecutor and multiprocessing.Queue if we ever start using
# those, too.
def _non_leaky_worker(executor_reference, work_queue):
    try:
        while True:
            try:
                work_item = work_queue.get(block=True, timeout=0.1)
            except concurrent.futures.thread.queue.Empty:
                executor = executor_reference()
                # Exit if:
                #   - The interpreter is shutting down OR
                #   - The executor that owns the worker has been collected OR
                #   - The executor that owns the worker has been shutdown.
                if concurrent.futures.thread._shutdown or executor is None or executor._shutdown:
                    return
                del executor
            else:
                work_item.run()
                del work_item # <= free this item before the next
                              #    work_queue.get call runs, rather than
                              #    after
    except BaseException:
        concurrent.futures.thread._base.LOGGER.critical('Exception in worker', exc_info=True)

concurrent.futures.thread._worker = _non_leaky_worker


def _force_quit(signum, frame):
    # traceback.print_stack(frame)
    os._exit(os.EX_IOERR)
    # os.abort()

def get_futures_threadpool(max_workers):
    #if force_quit_on_sigint:
    #    signal.signal(signal.SIGINT, _force_quit)
    return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

def wait_for_a_future(futures, print_traceback=False):
    '''
    Return the next future that completes.  If a KeyboardInterrupt is
    received, then the entire process is exited immediately.  See
    wait_for_all_futures for more notes.
    '''
    while True:
        try:
            future = concurrent.futures.as_completed(futures, timeout=10000000000).next()
            break
        except concurrent.futures.TimeoutError:
            pass
        except KeyboardInterrupt as e:
            if print_traceback:
                traceback.print_stack()
            else:
                print ''
            os._exit(os.EX_IOERR)

    return future

def wait_for_all_futures(futures, print_traceback=False):
    '''
    Wait indefinitely for all futures in the input iterable to complete.
    Use a timeout to enable interrupt handling.
    Call os._exit() in case of KeyboardInterrupt. Otherwise, the atexit registered handler in concurrent.futures.thread
    will run, and issue blocking join() on all worker threads, requiring us to listen to events in worker threads
    in order to enable timely exit in response to Ctrl-C.

    Note: This still doesn't handle situations where Ctrl-C is pressed elsewhere in the code and there are worker
    threads with long-running tasks.

    Note: os._exit() doesn't work well with interactive mode (e.g. ipython). This may help:
    import __main__ as main; if hasattr(main, '__file__'): os._exit() else: os.exit()
    '''
    try:
        while True:
            waited_futures = concurrent.futures.wait(futures, timeout=60)
            if len(waited_futures.not_done) == 0:
                break
    except KeyboardInterrupt as e:
        if print_traceback:
            traceback.print_stack()
        else:
            print ''
        os._exit(os.EX_IOERR)

def response_iterator(request_iterator, worker_pool, max_active_tasks=4):
    '''
    :param request_iterator: This is expected to be an iterator producing inputs for consumption by the worker pool.
    :param worker_pool: Assumed to be a concurrent.futures.Executor instance.
    :max_active_tasks: The maximum number of tasks that may be either running or waiting for consumption of their result.

    Rate-limited asynchronous multithreaded task runner.
    Consumes tasks from *request_iterator*. Yields their results in order, while allowing up to *max_active_tasks* to run
    simultaneously. Unlike concurrent.futures.Executor.map, prevents new tasks from starting while there are
    *max_active_tasks* or more unconsumed results.
    '''

    # Debug fallback
    #for _callable, args, kwargs in request_iterator:
    #    yield _callable(*args, **kwargs)
    #return

    future_deque = collections.deque()
    for i in range(max_active_tasks):
        try:
            _callable, args, kwargs = request_iterator.next()
            # print "Submitting (initial batch):", _callable, args, kwargs
            f = worker_pool.submit(_callable, *args, **kwargs)
            future_deque.append(f)
        except StopIteration:
            break

    while len(future_deque) > 0:
        f = future_deque.popleft()
        if not f.done():
            wait_for_all_futures([f])
        if f.exception() is not None:
            raise f.exception()
        try:
            _callable, args, kwargs = request_iterator.next()
            # print "Submitting", _callable, args, kwargs
            next_future = worker_pool.submit(_callable, *args, **kwargs)
            future_deque.append(next_future)
        except StopIteration:
            pass
        yield f.result()
        del f # Free the future we just consumed now, instead of next
              # time around the loop
        gc.collect()

def string_buffer_length(buf):
    orig_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(orig_pos)
    return buf_len

def normalize_time_input(t):
    ''' Converts inputs such as:
       "2012-05-01"
       "-5d"
       1352863174
    to milliseconds since epoch. See http://labix.org/python-dateutil and :meth:`normalize_timedelta`.
    '''
    error_msg = 'Error: Could not parse {t} as a timestamp or timedelta.  Expected a date format or an integer with a single-letter suffix: s=seconds, m=minutes, h=hours, d=days, w=weeks, M=months, y=years, e.g. "-10d" indicates 10 days ago'
    if isinstance(t, basestring):
        try:
            t = normalize_timedelta(t)
        except ValueError:
            try:
                t = int(time.mktime(dateutil.parser.parse(t).timetuple())*1000)
            except ValueError:
                raise ValueError(error_msg.format(t=t))
    if t < 0:
        t += int(time.time()*1000)
    return t

def normalize_timedelta(timedelta):
    '''
    Given a string like "1w" or "-5d", convert it to an integer in milliseconds.
    Note: not related to the datetime timedelta class.
    '''
    try:
        return int(timedelta)
    except ValueError:
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

    def __missing__ (self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        args = self.default_factory if self.default_factory else tuple()
        return type(self), args, None, None, self.items()
