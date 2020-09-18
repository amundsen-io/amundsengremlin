# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
largely lifted from tinkerpop/gremlin-python/src/main/java/org/apache/tinkerpop/gremlin/python/jsr223/PythonTranslator.java
and tinkerpop/gremlin-groovy/src/main/java/org/apache/tinkerpop/gremlin/groovy/jsr223/GroovyTranslator.java
all credit to its author, all blame leave here.
"""  # noqa: E501

import datetime
from abc import ABCMeta, abstractmethod
from itertools import starmap
from typing import (
    Any, Dict, Iterable, Iterator, List, Mapping, Sequence, Set, Union
)

from gremlin_python.driver.remote_connection import RemoteStrategy
from gremlin_python.process.traversal import (
    Barrier, Binding, Bytecode, Cardinality, Column, Direction,
    GraphSONVersion, GryoVersion, Operator, Order, P, Pick, Pop, Scope, T,
    Traversal
)
from gremlin_python.structure.graph import (
    Edge, Element, Vertex, VertexProperty
)
from overrides import overrides


class ScriptTranslator(metaclass=ABCMeta):
    @classmethod
    def translateB(cls, traversal_source: str, bytecode: Bytecode) -> str:
        return cls._internal_translate(traversal_source, bytecode)

    @classmethod
    def translateT(cls, traversal: Traversal) -> str:
        return cls._internal_translate(cls.get_traversal_source_name(traversal), traversal.bytecode)

    @classmethod
    def _internal_translate(cls, traversal_source: str, thing: Union[Traversal, Bytecode]) -> str:
        """
        Translates bytecode into a gremlin-groovy script.
        """
        if isinstance(thing, Traversal):
            bytecode = thing.bytecode
        else:
            bytecode = thing

        assert isinstance(bytecode, Bytecode), f'object is not supported!: {type(thing)} {thing}'
        return f'''{traversal_source}.{'.'.join(starmap(cls._translate_instruction, bytecode.step_instructions))}'''

    @classmethod
    def _translate_instruction(cls, step_name: str, *step_args: Any) -> str:
        assert isinstance(step_name, str), f'step_name is not a string? {step_name}'
        return f'''{step_name}({','.join(map(cls._convert_to_string, step_args))})'''

    @classmethod  # noqa: C901
    def _convert_to_string(cls, thing: Any) -> str:
        if thing is None:
            return "null"

        if isinstance(thing, bool):
            # TODO: this is java/groovy specific
            # also, did you know that isinstance(True, int) == True? so do this ahead of the int/float branch below
            return repr(thing).lower()

        if isinstance(thing, (int, float)):
            # TODO: do we need the f, L, d suffixes?
            return repr(thing)

        if isinstance(thing, str):
            return cls._escape_java_style(thing)

        if isinstance(thing, (Dict, Mapping)):
            return f'''[{','.join(f'({cls._convert_to_string(k)}):({cls._convert_to_string(v)})' for k, v in thing.items())}]'''  # noqa: E501

        if isinstance(thing, Set):
            return f'''[{','.join(cls._convert_to_string(i) for i in thing)}] as Set'''

        if isinstance(thing, (List, Sequence)):
            return f'''[{','.join(cls._convert_to_string(i) for i in thing)}]'''

        if isinstance(thing, Binding):
            binding: Binding = thing
            return cls._convert_to_string(binding.value)

        if isinstance(thing, Bytecode):
            return cls._internal_translate("__", thing)

        if isinstance(thing, Traversal):
            return cls._internal_translate("__", thing)

        if isinstance(thing, Element):
            if isinstance(thing, (Edge, Vertex, VertexProperty)):
                # returns like f'v[{thing.id}]' which seems right
                return repr(thing)

            raise AssertionError(f'thing is not supported!: {thing}')

        if isinstance(thing, P):
            p: P = thing
            # TODO: if isinstance(p, ConnectiveP)
            return f'{cls._qualify(type(p))}{p.operator}({cls._convert_to_string(p.value)})'

        if isinstance(thing, (Barrier, Cardinality, Column, Direction, GraphSONVersion, GryoVersion, Order, Pick, Pop,
                              Scope, Operator, T)):
            return f'{cls._qualify(type(thing))}{thing.name}'

        if isinstance(thing, (datetime.datetime, datetime.date)):
            return cls._date_to_string(thing)

        # TODO: Class, UUID?, Lambda, TraversalStrategyProxy, TraversalStrategy

        raise AssertionError(f'thing is not supported!: {thing}')

    @classmethod
    @abstractmethod
    def _date_to_string(cls, thing: Union[datetime.datetime, datetime.date]) -> str:
        raise RuntimeError('Not implemented')

    @classmethod
    def _qualify(cls, t: type) -> str:
        # TODO: for Neptune we would like to not qualify most things (e.g. except T, Order, Scope, all of which accept
        # the unqualified  so there's no point)
        return ''

    @classmethod
    def get_traversal_source_name(cls, t: Traversal) -> str:
        if t.traversal_strategies is not None:
            if t.traversal_strategies.traversal_strategies is not None:
                if isinstance(t.traversal_strategies.traversal_strategies[0], RemoteStrategy):
                    return t.traversal_strategies.traversal_strategies[0].remote_connection.traversal_source
                # TODO: more of these
        raise AssertionError(f'no idea what to do with {t}')

    CHAR_MAPPINGS = dict([(ord(v), f'\\{c}') for v, c in zip('\b\n\t\f\r', 'bntfr')]
                         + [(ord(s), f'\\{s}') for s in '\'"\\'])

    @classmethod
    def _escape_java_style_chars(cls, chars: Iterable[int]) -> Iterator[str]:
        for ch in chars:
            # handle unicode
            if ch in cls.CHAR_MAPPINGS:
                yield cls.CHAR_MAPPINGS[ch]
            elif ch < 0x7f and ch >= 32:
                yield chr(ch)
            else:
                # handle unicode and control characters
                yield f'''\\u{hex(ch)[2:].rjust(4, '0')}'''

    @classmethod
    def _escape_java_style(cls, value: str) -> str:
        return f'''"{''.join(cls._escape_java_style_chars(map(ord, value)))}"'''


class ScriptTranslatorTargetJanusgraph(ScriptTranslator):
    @classmethod
    @overrides
    def _date_to_string(cls, thing: Union[datetime.datetime, datetime.date]) -> str:
        if isinstance(thing, datetime.datetime):
            # datetime.datetime.isoformat(timespec='auto') does something a little surprising (that goes back into
            # antiquity). If milliseconds/microseconds == 0, then it OMITS them (as if yyyy-MM-dd'T'HH:mm:ss), which
            # is usually fine. Except here where we're passing it into java.text.SimpleDateFormat, which is strict.
            # so timespec='microseconds' to get those every time.
            return f'''new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").parse("{thing.isoformat(timespec='microseconds')}")'''  # noqa: E501
        elif isinstance(thing, datetime.date):
            # so timespec is not a thing for datetime.date though, so use the date only format produced there.
            return f'''new java.text.SimpleDateFormat("yyyy-MM-dd").parse("{thing.isoformat()}")'''
        else:
            raise AssertionError(f'thing is not supported!: {thing}')


class ScriptTranslatorTargetNeptune(ScriptTranslator):
    @classmethod
    @overrides
    def _date_to_string(cls, thing: Union[datetime.datetime, datetime.date]) -> str:
        return f'datetime("{thing.isoformat()}")'
