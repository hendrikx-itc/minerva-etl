from nose.tools import assert_raises, assert_true, \
        assert_false, assert_equal, raises

from minerva.util import compose, handle_exceptions, dict_to_handler


class TestException(Exception):
    pass


class UnhandledException(Exception):
    pass


def test_handle_exceptions():
    """
    The correct handler should be called
    """
    state = {}

    def fn():
        raise TestException("some error occurred")

    def handle_test_exception():
        state["val"] = 42

    handler_map = {TestException: handle_test_exception}

    handle = dict_to_handler(handler_map)

    handle_exceptions(handle, fn)()

    assert_equal(state["val"], 42)


@raises(UnhandledException)
def test_reraise_unhandled_exceptions():
    """
    Unhandled exceptions shoud be re-raised
    """
    def fn():
        raise UnhandledException("some error occurred")

    def handle_test_exception():
        state["val"] = 42

    handler_map = {TestException: handle_test_exception}

    handle = dict_to_handler(handler_map)

    handle_exceptions(handle, fn)()
