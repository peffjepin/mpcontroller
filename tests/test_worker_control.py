from .conftest import happens_soon
from .conftest import Controller
from .conftest import Worker
from .conftest import BlankWorker

import mpcontroller as mpc


def test_controller_has_no_worker_process_on_init():
    controller = BlankWorker.controller()

    assert controller.pid is None


def test_spawning_a_worker_from_the_class():
    controller = BlankWorker.spawn()

    assert controller.pid is not None


def test_spawning_a_worker_from_controller():
    controller = BlankWorker.controller()
    controller.spawn()

    assert controller.pid is not None


def test_specifying_controller_type():
    controller = BlankControllerWorker.spawn()

    assert isinstance(controller, BlankController)


def test_killing_a_worker():
    controller = BlankWorker.spawn()
    controller.kill()

    assert controller.pid is None


def test_killing_all_workers():
    controllers = [BlankWorker.spawn() for i in range(2)]
    mpc.kill_all()

    assert all(c.pid is None for c in controllers)


def test_killing_all_workers_of_a_given_type():
    w1 = [Worker1.spawn() for i in range(2)]
    w2 = [Worker2.spawn() for i in range(2)]

    mpc.kill_all(Worker1)

    assert all(c.pid is None for c in w1)

    assert all(c.pid is not None for c in w2)


def test_joining_a_worker():
    controller = BlankWorker.spawn()
    controller.join()

    @happens_soon
    def worker_joins():
        controller.pid is None


def test_joining_all_workers():
    controllers = [BlankWorker.spawn() for i in range(2)]
    mpc.join_all()

    @happens_soon
    def all_workers_join():
        assert all(c.pid is None for c in controllers)


def test_joining_all_workers_of_a_given_type():
    w1 = [Worker1.spawn() for i in range(2)]
    w2 = [Worker2.spawn() for i in range(2)]

    mpc.join_all(Worker1)

    @happens_soon
    def worker1_type_joins():
        assert all(c.pid is None for c in w1)

        assert all(c.pid is not None for c in w2)


class BlankController(Controller):
    pass


class BlankControllerWorker(Worker):
    CONTROLLER = BlankController


class Worker1(Worker):
    pass


class Worker2(Worker):
    pass
