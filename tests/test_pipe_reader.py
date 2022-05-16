import multiprocessing as mp

from .conftest import PipeReader
from .conftest import RecordedCallback
from .conftest import happens_soon
from .conftest import exception_soon
from .conftest import doesnt_happen
from .conftest import example_message
from .conftest import example_exception


class TestPipeReader:
    def setup_method(self):
        a, b = mp.Pipe()
        self.msg_cb = RecordedCallback()
        self.reader = PipeReader(a, message_handler=self.msg_cb)
        self.reader.start()
        self.conn = b

    def teardown_method(self):
        self.reader.join()

    def test_exception_should_be_raised_in_the_main_thread(self):
        @exception_soon(example_exception)
        def cause():
            self.conn.send(example_exception)

    def test_receiving_a_message(self):
        self.conn.send(example_message)

        @happens_soon
        def message_callback_invoked():
            assert self.msg_cb.called_with(example_message)

    def test_join(self):
        msg = "..."
        self.reader.join()
        self.conn.send(msg)

        @doesnt_happen
        def message_callback_invoked():
            assert self.msg_cb.called
