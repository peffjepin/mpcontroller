import pytest

from mpcontroller import util

from .conftest import happens_soon
from .conftest import VERY_FAST_TIMEOUT


@pytest.fixture(autouse=True, scope="function")
def cleanup_threads():
    yield
    for t in MainloopThread.threads:
        t.kill()
    MainloopThread.threads.clear()


class MainloopThread(util.MainloopThread):
    threads = []

    def __init__(self, *args, **kwargs):
        MainloopThread.threads.append(self)
        super().__init__(*args, **kwargs)


def test_mainloop_executes_on_loop():
    class Thread(MainloopThread):
        n = 0

        def mainloop(self):
            self.n += 1

    t = Thread()
    t.start()

    @happens_soon
    def mainloop_called_multiple_times():
        assert t.n >= 2


def test_mainloop_thread_joins():
    class Thread(MainloopThread):
        def mainloop(self):
            pass

    t = Thread()
    t.start()
    t.join(VERY_FAST_TIMEOUT)


def test_mainloop_thread_kill_results_in_thread_stopping():
    class Thread(MainloopThread):
        def mainloop(self):
            pass

    t = Thread()
    t.start()
    t.kill()

    @happens_soon
    def thread_is_no_longer_running():
        assert not t.is_alive()
