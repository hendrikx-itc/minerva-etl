# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from contextlib import closing

from minerva.util import no_op


DEFAULT_MAX_RETRIES = 10


class MaxRetriesError(Exception):
    pass


class DbTransaction(object):
    """
    A list of actions on a database that can be executed in sequence and will
    either succeed or fail completely.
    """
    def __init__(self, *args):
        self.actions = list(args)

    def __str__(self):
        return " -> ".join([a.__class__.__name__ for a in self.actions])

    def execute(self, cursor):
        state = {}

        for i, action in enumerate(self.actions):
            logging.debug("{}. {}".format(i, type(action).__name__))
            modification = action.execute(cursor, state)
            logging.debug(modification)

            if modification:
                return modification(action, self)

    def append(self, action):
        """
        Append `action` to this transaction and return this transaction.
        """
        self.actions.append(action)

        return self

    def insert_before(self, ref_action, action):
        ref_index = self.actions.index(ref_action)

        self.actions.insert(ref_index, action)

    def replace(self, action, replacement_action):
        index = self.actions.index(action)

        self.actions[index] = replacement_action

    def drop(self, action):
        """
        Drop (remove) `action` from this transaction.
        """
        self.actions.remove(action)

    def extend(self, transaction):
        """
        Extend this transaction with all actions from `transaction` and return
        this transaction.
        """
        self.actions.extend(transaction.actions)

        return self

    def run(self, conn, max_retries=DEFAULT_MAX_RETRIES):
        transaction = self
        attempt = 1

        with closing(conn.cursor()) as cursor:
            while transaction:
                logging.debug("DbTransaction({})".format(transaction))
                transaction = transaction.execute(cursor)

                if transaction:
                    conn.rollback()

                    if attempt > max_retries:
                        msg = (
                            "maximum number({}) of retries reached with "
                            "current transaction [{}]"
                        ).format(max_retries, transaction)

                        raise MaxRetriesError(msg)

                attempt += 1

        conn.commit()


class DbAction(object):
    def execute(self, cursor, state):
        """
        Override in subclass. Should return a Fix if it fails, or None
        otherwise. A fix is a function that receives two arguments: The current
        action, and the transaction object.
        """
        raise NotImplementedError()


class UpdateState(DbAction):
    def __init__(self, update_fn=no_op):
        self.update_fn = update_fn

    def execute(self, cursor, state):
        """
        Update the state using the update_fn
        """
        self.update_fn(state)


class WithState(DbAction):
    def __init__(self, f, arg_funcs):
        self.arg_funcs = arg_funcs
        self.f = f

    def execute(self, cursor, state):
        """
        Create action action_type with state variables as arguments to create
        the actual action.
        """
        args = [arg_func(state) for arg_func in self.arg_funcs]

        return self.f(*args)(cursor)


def insert_before(new_action):
    def fn(action, transaction):
        transaction.insert_before(action, new_action)

        return transaction

    return fn


def replace(replacement_action):
    def fn(action, transaction):
        transaction.replace(action, replacement_action)

        return transaction

    return fn


def drop_action():
    def fn(action, transaction):
        transaction.drop(action)

        return transaction

    return fn
