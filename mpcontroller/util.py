import threading
import time
import heapq

from collections import defaultdict

from . import global_state


class MethodMarker:
    _registry = defaultdict(lambda: defaultdict(list))

    def __init_subclass__(cls):
        cls._registry = defaultdict(lambda: defaultdict(list))

    def __init__(self, key):
        self._key = key

    def __call__(self, fn):
        self._fn = fn
        return self

    def __set_name__(self, cls, name):
        setattr(cls, name, self._fn)
        self._register(name, cls)

    def _register(self, name, cls):
        self._registry[cls][self._key].append(name)

    @classmethod
    def get_registered_keys(cls, type):
        return cls._registry[type].keys()

    @classmethod
    def make_callback_table(cls, object):
        classes_to_check = type(object).__mro__[:-1]
        callbacks_seen = set()

        table = defaultdict(list)

        for c in classes_to_check:
            for key, names in cls._registry[c].items():
                for name in names:
                    if name in callbacks_seen:
                        continue
                    callbacks_seen.add(name)
                    bound_method = getattr(object, name)
                    table[key].append(bound_method)

        return table


class MainloopThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        self._running = False
        super().__init__(*args, target=self._main, daemon=True, **kwargs)

    def _main(self):
        self._running = True
        while self._running:
            self.mainloop()
            global_state.clock.tick(self)

    def join(self, timeout=None):
        self._running = False
        super().join(timeout)

    def kill(self):
        self._running = False

    def mainloop(self):
        raise NotImplementedError()


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
    def __init__(self, interval=None):
        self._consumers = dict()
        self._interval = interval or global_state.config.poll_interval

    def tick(self, consumer):
        if consumer not in self._consumers:
            time.sleep(self._interval)
            self._consumers[consumer] = time.time() + self._interval
        else:
            next = self._consumers[consumer]
            time.sleep(max(0, next - time.time()))
            self._consumers[consumer] = time.time() + self._interval
