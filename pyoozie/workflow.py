# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import abc
import copy
import typing  # pylint: disable=unused-import

from pyoozie import tags  # pylint: disable=unused-import


# TODO auto-generate Oozie compatible ids


class AbstractIdentifiedNode(object):
    """Workflow tags representing nodes in a workflow's execution, e.g. Kill, and Action"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, identifier):
        # type: (AbstractAction) -> None
        self.__identifier = identifier

    def identifier(self):
        return self.__identifier


class AbstractAction(AbstractIdentifiedNode):
    """Parent class of both Action and action collections"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, identifier=None, on_error=None):
        # type: (str, AbstractIdentifiedNode) -> None
        super(AbstractAction, self).__init__(identifier=identifier or 'auto-generated-id-here')
        self.__on_error = copy.deepcopy(on_error)

    def on_error(self):
        return self.__on_error


class Action(AbstractAction):
    """Concrete action to execute (takes an action tag, e.g. pyoozie.Shell, pyoozie.Email. etc.)"""

    def __init__(self, action_tag, on_error=None):
        # type: (typing.Union[tags.Email, tags.Shell, tags.SubWorkflow], AbstractIdentifiedNode) -> None
        super(Action, self).__init__(identifier='auto-generated', on_error=on_error)
        self.action_tag = action_tag


class Serial(AbstractAction):
    """Collection of actions to execute sequentially (implemented by chaining actions and 'OK' transitions)"""

    def __init__(self, *actions, **kwargs):
        # type: (typing.Tuple[AbstractAction, ...], typing.Dict[str, AbstractIdentifiedNode]) -> None
        super(Serial, self).__init__(identifier='auto-generated', on_error=kwargs.get('on_error'))
        self.actions = copy.deepcopy(actions)


class Parallel(AbstractAction):
    """Set of actions to execute in parallel (implemented as fork/join tag pair)"""

    def __init__(self, *actions, **kwargs):
        # type: (typing.Tuple[AbstractAction, ...], typing.Dict[str, AbstractIdentifiedNode]) -> None
        super(Parallel, self).__init__(identifier='auto-generated', on_error=kwargs.get('on_error'))
        self.actions = set(copy.deepcopy(actions))


class Switch(AbstractAction):
    """Set of JSP Expression Language case statements and actions to follow as a result"""

    def __init__(self, cases, default):
        # type: Dict[str, AbstractIdentifiedNode], AbstractIdentifiedNode) -> None
        super(Switch, self).__init__(identifier='auto-generated', on_error=None)
        self.cases = set(copy.deepcopy(cases))
        self.default = copy.deepcopy(default)


class Kill(AbstractIdentifiedNode):
    """Stop execution immediately"""

    def __init__(self, message):
        super(Kill, self).__init__(identifier='auto-generated')
        self.__message = message
