# -*- coding: utf-8 -*-
import unittest

from minerva.util import identity, compose, k, no_op, retry_while, zip_apply


class TestFunctional(unittest.TestCase):
    def test_identity(self):
        self.assertEqual(identity(42), 42)
        self.assertEqual(identity("Hello world!"), "Hello world!")
        self.assertEqual(identity((1, 2, 3)), (1, 2, 3))

    def test_compose_pair(self):
        composed = compose(times_two, add_one)

        self.assertEqual(composed(2), 6)

    def test_compose(self):
        composed = compose(add_one, times_two, add_one, add_one)

        self.assertEqual(composed(1), 7)

    def test_k(self):
        """
        The result of the created function should always be the same
        """
        r = k(5)

        self.assertEqual(r(), 5)
        self.assertEqual(r(2), 5)
        self.assertEqual(r("hello", 3), 5)

    def test_retry_while(self):
        state = {
            "loop": 1,
            "val": 10}

        exception_handlers = {
            Exception: no_op}

        def fn():
            curr_loop = state["loop"]
            state["loop"] = curr_loop + 1

            if curr_loop == 1:
                raise Exception()

            state["val"] = 42

        retry_while(fn, exception_handlers, condition=k(True), timeout=k(1.0))

        self.assertEqual(state["val"], 42)

    def test_zip_apply(self):
        funcs = (add_one, add_one, times_two)
        values = (1, 2, 3)
        result = zip_apply(funcs)(values)

        self.assertEqual(result, [2, 3, 6])

        result = zip_apply([])([])

        assert result == []


def add_one(x):
    return x + 1


def times_two(x):
    return x * 2
