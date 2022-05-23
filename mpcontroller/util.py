import threading
import time

from collections import defaultdict

from mpcontroller import global_state


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
            time.sleep(global_state.config.poll_interval)

    def join(self, timeout=None):
        self._running = False
        super().join(timeout)

    def kill(self):
        self._running = False

    def mainloop(self):
        raise NotImplementedError()
