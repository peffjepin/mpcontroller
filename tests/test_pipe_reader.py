import multiprocessing as mp

from .conftest import PipeReader, RecordedCallback, happens_soon, doesnt_happen


class TestPipeReader:
    def setup_method(self):
        a, b = mp.Pipe()
        self.msg_cb = RecordedCallback()
        self.exc_cb = RecordedCallback()
        self.reader = PipeReader(
            a, exception_handler=self.exc_cb, message_handler=self.msg_cb
        )
        self.reader.start()
        self.conn = b

    def teardown_method(self):
        self.reader.join()

    def test_receiving_an_exception(self, exception):
        self.conn.send(exception)

        @happens_soon
        def triggers_exc_callback():
            assert self.exc_cb.called
            assert self.exc_cb.arg == exception

    def test_receiving_a_message(self):
        msg = "Anything that isn't an exception"
        self.conn.send(msg)

        @happens_soon
        def message_callback_invoked():
            assert self.msg_cb.called
            assert self.msg_cb.arg == msg

    def test_join(self):
        msg = "..."
        self.reader.join()
        self.conn.send(msg)

        @doesnt_happen
        def message_callback_invoked():
            assert self.msg_cb.called
