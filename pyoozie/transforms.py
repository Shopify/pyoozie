# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import six


MESSAGE_ON_KILL = '${wf:lastErrorNode()} - ${wf:id()}'


def serial_layout(env, error_to_kill=MESSAGE_ON_KILL):
    """
    TODO
    1. Validate that all dependencies are provided
    2. Construct a linear sequence of actions by performing these steps:
      2a. Add all actions with no dependencies
      2b. For all actions added, remove them as dependencies for un-added actions
      3c. If there are no actions left to add, return, else go to 2a
    3. If `error_to_kill` add a kill node and route all errors to it else route all errors to the next node
    """
    assert 'workflow' in env
    assert 'dependencies' in env
    assert error_to_kill is None or isinstance(error_to_kill, six.string_types)
    return env


def fork_join_layout(env):
    """
    TODO
    """
    return env


def add_actions_on_error(env):
    """
    TODO
    Relies upon action kwargs to add on-failure actions and kill nodes:
      - action_on_error (action)
      - kill_on_error (message, optional)
    Validate kwargs and then then for each action in env['action']:
      1. Create/select a kill node to end at (if `kill_on_error`)
      2. Set error transition to go to `action_on_error`
      3. Set ok/error for `action_on_error` to the kill node chosen OR to the original action's error node

    Use cases:
      - Notifying on an error and then optionally going to kill
    """
    assert 'workflow' in env
    return env


def final_action(env, action):
    """
    TODO
    1. Find 'end' node
    2. Insert action node before end (on ok), route ok and error to end

    Use cases:
      - Notifying on workflow success
    """
    assert action
    return env
