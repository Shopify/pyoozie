# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import typing  # pylint: disable=unused-import

from pyoozie import tags  # pylint: disable=unused-import


class AbstractAction(object):
    pass


class Action(AbstractAction):
    """Concrete action to execute (takes an action tag, e.g. pyoozie.Shell, pyoozie.Email. etc.)"""

    def __init__(self, action_tag, on_error=None):
        # type: (tags.Shell, AbstractAction) -> None
        self.__action_tag = action_tag
        self.__on_error = on_error


class Sequence(AbstractAction):
    """Set of actions to execute sequentially (implemented by chaining actions and 'OK' transitions)"""

    def __init__(self, *actions, **kwargs):
        # type: (typing.Tuple[AbstractAction, ...], typing.Dict[str, AbstractAction]) -> None
        self.__on_error = kwargs.get('on_error')
        self.__actions = actions


class Parallelization(AbstractAction):
    """Set of actions to execute in parallel (implemented as fork/join tag pair)"""

    def __init__(self, *actions, **kwargs):
        # type: (typing.Tuple[AbstractAction, ...], typing.Dict[str, AbstractAction]) -> None
        self.__on_error = kwargs.get('on_error')
        self.__actions = set(actions)


class Kill(AbstractAction):
    """Stop execution immediately"""

    def __init__(self, message):
        self.__message = message
