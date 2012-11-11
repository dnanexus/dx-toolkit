'''
Utilities shared by dxpy modules.
'''

import os, sys, collections, concurrent.futures, signal, traceback
from exec_utils import *

def _force_quit(signum, frame):
    traceback.print_stack(frame)
    os._exit(os.EX_IOERR)
    # os.abort()

def get_futures_threadpool(max_workers):
    #if force_quit_on_sigint:
    #    signal.signal(signal.SIGINT, _force_quit)
    return concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

def wait_for_all_futures(futures):
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
        traceback.print_stack()
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

def string_buffer_length(buf):
    orig_pos = buf.tell()
    buf.seek(0, os.SEEK_END)
    buf_len = buf.tell()
    buf.seek(orig_pos)
    return buf_len

def normalize_timedelta(timedelta):
    '''
    Given a string like "1w" or "-5d", convert it to an integer in milliseconds.
    Note: not related to the datetime timedelta class.
    '''
    error_msg = "Error: Could not parse \"" + str(timedelta) + "\" as a timestamp or timedelta.  Expected either an integer with no suffix or with a single-letter suffix: s=seconds, m=minutes, h=hours, d=days, w=weeks, M=months, y=years, e.g. \"-10d\" indicates 10 days ago"
    try:
        return int(timedelta)
    except ValueError:
        t, suffix = timedelta[:-1], timedelta[-1:]
        suffix_multipliers = {'s': 1000, 'm': 1000*60, 'h': 1000*60*60, 'd': 1000*60*60*24, 'w': 1000*60*60*24*7,
                              'M': 1000*60*60*24*30, 'y': 1000*60*60*24*365}
        if suffix not in suffix_multipliers:
            raise ValueError(error_msg)
        try:
            return int(t) * suffix_multipliers[suffix]
        except ValueError:
            raise ValueError(error_msg)

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
