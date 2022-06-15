import threading
import time
import heapq

from collections import defaultdict

from . import config


class MethodMarker:
    _registry = defaultdict(lambda: defaultdict(list))

    def __init__(self):
        self._registry = defaultdict(lambda: defaultdict(list))

    def mark(self, key):
        def capture_fn(fn):
            return _MarkedFunctionWrapper(fn, key, self._registry)

        return capture_fn

    def get_registered_keys(self, type):
        return self._registry[type].keys()

    def make_callback_table(self, object):
        classes_to_check = type(object).__mro__[:-1]
        callbacks_seen = set()

        table = defaultdict(list)

        for c in classes_to_check:
            for key, names in self._registry[c].items():
                for name in names:
                    if name in callbacks_seen:
                        continue
                    callbacks_seen.add(name)
                    bound_method = getattr(object, name)
                    table[key].append(bound_method)

        return table


class _MarkedFunctionWrapper:
    def __init__(self, fn, key, registry):
        self._fn = fn
        self._key = key
        self._registry = registry

    def __set_name__(self, cls, name):
        setattr(cls, name, self._fn)
        self._registry[cls][self._key].append(name)


class Schedule:
    def __init__(self, interval_callbacks_pairs):
        self._timeouts = []
        now = time.time()
        for interval, fns in interval_callbacks_pairs:
            self._push(now, interval, fns)

    def _push(self, deadline, interval, callbacks):
        item = (deadline, interval, callbacks)
        heapq.heappush(self._timeouts, item)

    def _pop(self):
        return heapq.heappop(self._timeouts)

    def update(self):
        now = time.time()

        deadline, interval, callbacks = self._pop()
        while now > deadline:
            for fn in callbacks:
                fn()
            self._push(time.time() + interval, interval, callbacks)
            deadline, interval, callbacks = self._pop()

        self._push(time.time() + interval, interval, callbacks)


class Clock:
    def __init__(self, interval=None, context=None):
        self._consumers = dict()
        self._context = context
        self._interval = interval

    def tick(self, consumer):
        interval = self._actual_interval
        if consumer not in self._consumers:
            time.sleep(interval)
            self._consumers[consumer] = time.time() + interval
        else:
            next = self._consumers[consumer]
            time.sleep(max(0, next - time.time()))
            self._consumers[consumer] = time.time() + interval

    @property
    def _actual_interval(self):
        if self._context is not None:
            return self._context.poll_interval
        if self._interval is None:
            return config.local_context.poll_interval
        return self._interval


clock = Clock()


class MainloopThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        self._running = False
        super().__init__(*args, target=self._main, daemon=True, **kwargs)

    def _main(self):
        self._running = True
        while self._running:
            self.mainloop()
            clock.tick(self)

    def join(self, timeout=None):
        self._running = False
        super().join(timeout)

    def kill(self):
        self._running = False

    def mainloop(self):
        raise NotImplementedError()
