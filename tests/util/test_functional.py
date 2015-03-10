# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.test import assert_equal
from minerva.util import identity, compose, k, no_op, retry_while, zip_apply


def test_identity():
    assert_equal(identity(42), 42)
    assert_equal(identity("Hello world!"), "Hello world!")
    assert_equal(identity((1, 2, 3)), (1, 2, 3))


def test_compose_pair():
    composed = compose(times_two, add_one)

    assert_equal(composed(2), 6)


def test_compose():
    composed = compose(add_one, times_two, add_one, add_one)

    assert_equal(composed(1), 7)


def test_k():
    """
    The result of the created function should always be the same
    """
    r = k(5)

    assert_equal(r(), 5)
    assert_equal(r(2), 5)
    assert_equal(r("hello", 3), 5)


def test_retry_while():
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

    assert_equal(state["val"], 42)


def test_zipapply():
    funcs = (add_one, add_one, times_two)
    values = (1, 2, 3)
    result = zip_apply(funcs)(values)

    assert_equal(result, [2, 3, 6])

    result = zip_apply([])([])

    assert result == []


def add_one(x):
    return x + 1


def times_two(x):
    return x * 2
