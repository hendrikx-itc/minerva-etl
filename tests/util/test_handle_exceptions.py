import unittest

from minerva.util import handle_exceptions, dict_to_handler


class NormalException(Exception):
    pass


class UnhandledException(Exception):
    pass


class TestHandleExceptions(unittest.TestCase):
    def test_handle_exceptions(self):
        """
        The correct handler should be called
        """
        state = {}

        def fn():
            raise NormalException("some error occurred")

        def handle_test_exception():
            state["val"] = 42

        handler_map = {NormalException: handle_test_exception}

        handle = dict_to_handler(handler_map)

        handle_exceptions(handle, fn)()

        self.assertEqual(state["val"], 42)

    def test_reraise_unhandled_exceptions(self):
        """
        Unhandled exceptions shoud be re-raised
        """
        state = {}

        def fn():
            raise UnhandledException("some error occurred")

        def handle_test_exception():
            state["val"] = 42

        handler_map = {NormalException: handle_test_exception}

        handle = dict_to_handler(handler_map)

        with self.assertRaises(UnhandledException) as cm:
            handle_exceptions(handle, fn)()
