# Copyright (C) 2013-2014 DNAnexus, Inc.
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

"""This module contains PrioritizingThreadPool, an implementation of an
interface similar to that of concurrent.futures.Executor. See:
https://docs.python.org/dev/library/concurrent.futures.html

"""


import collections
import concurrent.futures
import threading


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
            work_item = work_queue.get(block=True)
            if work_item is not None:
                work_item.run()
                del work_item
                continue
            executor = executor_reference()
            # Exit if:
            #   - The interpreter is shutting down OR
            #   - The executor that owns the worker has been collected OR
            #   - The executor that owns the worker has been shutdown.
            if concurrent.futures.thread._shutdown or executor is None or executor._shutdown:
                # Notice other workers
                work_queue.put(None)
                return
            del executor
    except BaseException:
        concurrent.futures.thread._base.LOGGER.critical('Exception in worker', exc_info=True)

def _chain_result(outer_future):
    """Returns a callable that can be supplied to Future.add_done_callback
    to propagate a future's result to outer_future.

    """
    def f(inner_future):
        try:
            result = inner_future.result()
        except BaseException as e:
            outer_future.set_exception(e)
        else:
            outer_future.set_result(result)
    return f

concurrent.futures.thread._worker = _non_leaky_worker


def _run_callable_with_postamble(postamble, callable_, *args, **kwargs):
    """Returns a callable of no args that invokes callable_ (with the
    specified args and kwargs) and then invokes postamble (with no
    args).

    The callable returns the result of (or exception thrown by)
    callable_.

    """
    def fn():
        try:
            return callable_(*args, **kwargs)
        finally:
            postamble()
    return fn

class PrioritizingThreadPool(object):
    """Presents an abstraction similar to that of
    concurrent.futures.Executor except that multiple clients may write
    their tasks to separate queues (which may be distinguished by any
    hashable object). Tasks are handled by different threads (in the
    same process) simultaneously. The tasks in each queue are processed
    in order; tasks written to different queues are processed as
    follows:

    When a task is submitted using submit_to_queue the client may
    specify a priority_fn to go along with that task. Each time a worker
    thread is ready to start a task, the priority_fn of each candidate
    task (the head of each queue) is called, and the task that returns
    the lowest value is chosen. (This is more generic than a priority
    queue in that the priority value of each task is not a static value
    that must be submitted at the time that the task is enqueued.)

    When a task is enqueued, we return a Future for the result of that
    task.

    """

    def __init__(self, max_workers):
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._tasks = threading.Semaphore(max_workers)
        self._queue_lock = threading.Lock()
        # Invariant: self._queues is a mapping of queue_id to a NONEMPTY
        # list of Futures representing yet-unscheduled items in that
        # queue. (This invariant may only be violated by threads that
        # are holding _queue_lock.)
        #
        # Each Future is the future we gave to the client, augmented
        # with:
        # (1) a field "args" containing a tuple
        #     (callable, args, kwargs), and
        # (2) a field "priority_fn" with the priority function for that
        #     task.
        self._queues = {}

    def _submit_one(self, callable_, *args, **kwargs):
        """Starts the next task (which, when complete, will, in turn, start one
        more task when finished, which will, in turn, etc.). Returns a
        future object corresponding to the newly started task.

        Thread safety note: assumes that the caller has already reserved
        a worker using self._tasks.

        """
        def postamble():
            self._tasks.release()
            self._maybe_schedule_task()
        return self._pool.submit(_run_callable_with_postamble(postamble, callable_, *args, **kwargs))

    def _maybe_schedule_task(self):
        """Starts a task if there is an available worker to serve it.

        Thread safe.

        """
        if self._tasks.acquire(blocking=False):
            # Atomically remove the item from the queue and feed it to
            # the ThreadPoolExecutor.
            self._queue_lock.acquire()
            try:
                outer_future = self._next()
            except StopIteration:
                # Oops, there is in fact no task to be served, so we
                # won't be tying up a worker after all.
                self._tasks.release()
            else:
                callable_, args, kwargs = outer_future.args
                inner_future = self._submit_one(callable_, *args, **kwargs)
                # Now that we have the real future (inner_future), chain
                # its result to what we provided to our client
                inner_future.add_done_callback(_chain_result(outer_future))
            finally:
                self._queue_lock.release()

    def _next(self):
        """Pop the highest priority task.

        Returns the Future corresponding to that task (and removes it
        from the queue of items to be scheduled), or raises
        StopIteration if no tasks are available.

        Thread safety note: assumes the caller is holding
        self._queue_lock (the caller will probably also want to hold the
        same lock while scheduling the result of this method, so as to
        make the pop+schedule operation atomic).

        """
        if self._queue_lock.acquire(False):
            raise AssertionError('Expected _queue_lock to be held here')

        queue_ids = list(self._queues.keys())
        if not queue_ids:
            raise StopIteration()

        # Find the queue whose head item has the lowest priority value
        best_queue_id = None
        best_priority_value = None
        for candidate_queue_id in queue_ids:
            selected_queue = self._queues[candidate_queue_id]
            if not len(selected_queue):
                raise AssertionError('Invariant violation: queue %r is empty' % (candidate_queue_id,))
            head_of_queue = selected_queue[0]
            priority_value = head_of_queue.priority_fn() if head_of_queue.priority_fn else 0
            if best_queue_id is None or priority_value < best_priority_value:
                best_queue_id = candidate_queue_id
                best_priority_value = priority_value
        queue_id = best_queue_id
        assert queue_id is not None

        next_task = self._queues[queue_id].popleft()
        if len(self._queues[queue_id]) == 0:
            del self._queues[queue_id]
        return next_task

    def submit(self, callable_, *args, **kwargs):
        """For compatibility with code that was previously using
        ThreadPoolExecutor directly, provides a similar interface to the
        submit method of that class.

        Requests submitted in this way have a priority of 0 and go into
        a single default queue.

        Returns a Future corresponding to the specified task.

        """
        return self.submit_to_queue('', None, callable_, *args, **kwargs)

    def submit_to_queue(self, queue_id, priority_fn, callable_, *args, **kwargs):
        """Adds a new task to the end of the specified queue.

        Returns a Future corresponding to the specified task.

        :param queue_id: indicates which queue this request should go at
        the end of
        :param priority_fn: a function of no args. Whenever a worker is
        available, the task whose priority_fn returns the lowest value
        is selected. None may also be provided in which case the
        priority_fn is considered to return 0.

        """
        if queue_id is None:
            # In _next, None is used as a sentinel value
            raise AssertionError('queue_id may not be None')

        outer_future = concurrent.futures._base.Future()
        outer_future.priority_fn = priority_fn
        outer_future.args = (callable_, args, kwargs)
        with self._queue_lock:
            if queue_id not in self._queues:
                self._queues[queue_id] = collections.deque()
            self._queues[queue_id].append(outer_future)

        # Start the task now if there is a worker that can serve it.
        self._maybe_schedule_task()

        return outer_future
