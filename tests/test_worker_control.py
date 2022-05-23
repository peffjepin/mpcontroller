from .conftest import happens_soon
from .conftest import BlankWorker

import mpcontroller as mpc


def test_spawning_a_worker_from_the_class():
    worker = BlankWorker.spawn()

    assert worker.pid is not None


def test_starting_a_worker_like_a_process():
    worker = BlankWorker()
    worker.start()

    assert worker.pid is not None


def test_killing_a_worker():
    worker = BlankWorker.spawn()
    worker.kill()

    assert worker.pid is None


def test_killing_all_workers():
    workers = [BlankWorker.spawn() for i in range(2)]
    mpc.kill_all()

    assert all(worker.pid is None for worker in workers)


def test_killing_all_workers_of_a_given_type():
    worker1_list = [Worker1.spawn() for i in range(2)]
    worker2_list = [Worker2.spawn() for i in range(2)]

    mpc.kill_all(Worker1)

    assert all(worker.pid is None for worker in worker1_list)
    assert all(worker.pid is not None for worker in worker2_list)


def test_joining_a_worker():
    worker = BlankWorker.spawn()
    worker.join()

    @happens_soon
    def worker_joins():
        worker.pid is None


def test_joining_all_workers():
    workers = [BlankWorker.spawn() for i in range(2)]
    mpc.join_all()

    @happens_soon
    def all_workers_join():
        assert all(worker.pid is None for worker in workers)


def test_joining_all_workers_of_a_given_type():
    worker1_list = [Worker1.spawn() for i in range(2)]
    worker2_list = [Worker2.spawn() for i in range(2)]

    mpc.join_all(Worker1)

    @happens_soon
    def worker1_type_joins():
        assert all(worker.pid is None for worker in worker1_list)
        assert all(worker.pid is not None for worker in worker2_list)


class Worker1(mpc.Worker):
    pass


class Worker2(mpc.Worker):
    pass
