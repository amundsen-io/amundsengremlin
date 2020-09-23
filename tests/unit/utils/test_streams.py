# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import sys
import unittest
from typing import AsyncIterator, Iterable
from unittest.mock import Mock, call

import pytest

from amundsen_gremlin.utils.streams import (
    PeekingIterator, _assure_collection, async_consume_in_chunks,
    consume_in_chunks, consume_in_chunks_with_state, one_chunk,
    reduce_in_chunks
)


class TestConsumer(unittest.TestCase):
    def test_consume_in_chunks(self) -> None:
        values = Mock()
        values.side_effect = list(range(5))
        consumer = Mock()
        parent = Mock()
        parent.values = values
        parent.consumer = consumer

        def stream() -> Iterable[int]:
            for _ in range(5):
                yield values()

        count = consume_in_chunks(stream=stream(), n=2, consumer=consumer)
        self.assertEqual(count, 5)
        # this might look at little weird, but PeekingIterator is why
        self.assertSequenceEqual([call.values(), call.values(), call.values(), call.consumer((0, 1)),
                                  call.values(), call.values(), call.consumer((2, 3)), call.consumer((4,))],
                                 parent.mock_calls)

    def test_consume_in_chunks_with_exception(self) -> None:
        consumer = Mock()

        def stream() -> Iterable[int]:
            yield from range(10)
            raise KeyError('hi')

        with self.assertRaisesRegex(KeyError, 'hi'):
            consume_in_chunks(stream=stream(), n=4, consumer=consumer)
        self.assertSequenceEqual([call.consumer((0, 1, 2, 3)), call.consumer((4, 5, 6, 7))], consumer.mock_calls)

    def test_consume_in_chunks_with_state(self) -> None:
        values = Mock()
        values.side_effect = list(range(5))
        consumer = Mock()
        consumer.side_effect = list(range(1, 4))
        state = Mock()
        state.side_effect = lambda x: x * 10
        parent = Mock()
        parent.values = values
        parent.consumer = consumer
        parent.state = state

        def stream() -> Iterable[int]:
            for _ in range(5):
                yield values()

        result = consume_in_chunks_with_state(stream=stream(), n=2, consumer=consumer, state=state)
        self.assertSequenceEqual(tuple(result), (0, 10, 20, 30, 40))
        # this might look at little weird, but PeekingIterator is why
        self.assertSequenceEqual([call.values(), call.values(), call.values(), call.state(0), call.state(1),
                                  call.consumer((0, 1)), call.values(), call.values(), call.state(2), call.state(3),
                                  call.consumer((2, 3)), call.state(4), call.consumer((4,))],
                                 parent.mock_calls)

    def test_consume_in_chunks_no_batch(self) -> None:
        consumer = Mock()
        count = consume_in_chunks(stream=range(100000000), n=-1, consumer=consumer)
        self.assertEqual(100000000, count)
        consumer.assert_called_once()

    def test_reduce_in_chunks(self) -> None:
        values = Mock()
        values.side_effect = list(range(5))
        consumer = Mock()
        consumer.side_effect = list(range(1, 4))
        parent = Mock()
        parent.values = values
        parent.consumer = consumer

        def stream() -> Iterable[int]:
            for _ in range(5):
                yield values()

        result = reduce_in_chunks(stream=stream(), n=2, initial=0, consumer=consumer)
        self.assertEqual(result, 3)
        # this might look at little weird, but PeekingIterator is why
        self.assertSequenceEqual([call.values(), call.values(), call.values(), call.consumer((0, 1), 0),
                                  call.values(), call.values(), call.consumer((2, 3), 1), call.consumer((4,), 2)],
                                 parent.mock_calls)

    @pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
    def test_async_consume_in_chunks(self) -> None:
        values = Mock()
        values.side_effect = list(range(5))
        consumer = Mock()
        parent = Mock()
        parent.values = values
        parent.consumer = consumer

        async def stream() -> AsyncIterator[int]:
            for i in range(5):
                yield values()

        count = asyncio.run(async_consume_in_chunks(stream=stream(), n=2, consumer=consumer))
        self.assertEqual(5, count, 'count')
        # this might look at little weird, but PeekingIterator is why
        self.assertSequenceEqual([call.values(), call.values(), call.values(), call.consumer((0, 1)),
                                  call.values(), call.values(), call.consumer((2, 3)), call.consumer((4,))],
                                 parent.mock_calls)

    def test_one_chunk_logging(self) -> None:
        it = PeekingIterator(range(1, 4))
        actual, has_more = one_chunk(it=it, n=2, metric=lambda x: x)
        self.assertSequenceEqual([1], tuple(actual))
        self.assertTrue(has_more)

        actual, has_more = one_chunk(it=it, n=2, metric=lambda x: x)
        self.assertSequenceEqual([2], tuple(actual))
        self.assertTrue(has_more)

        with self.assertLogs(logger='amundsen_gremlin.utils.streams', level=logging.ERROR):
            actual, has_more = one_chunk(it=it, n=2, metric=lambda x: x)
        self.assertSequenceEqual([3], tuple(actual))
        self.assertFalse(has_more)

    def test_assure_collection(self) -> None:
        actual = _assure_collection(iter(range(2)))
        self.assertIsInstance(actual, tuple)
        self.assertEqual((0, 1), actual)
        actual = _assure_collection(list(range(2)))
        self.assertIsInstance(actual, list)
        self.assertEqual([0, 1], actual)
        actual = _assure_collection(set(range(2)))
        self.assertIsInstance(actual, set)
        self.assertEqual({0, 1}, actual)
        actual = _assure_collection(frozenset(range(2)))
        self.assertIsInstance(actual, frozenset)
        self.assertEqual(frozenset({0, 1}), actual)


class TestPeekingIterator(unittest.TestCase):
    # TODO: it'd be good to test the locking
    def test_no_peek(self) -> None:
        it = PeekingIterator(range(3))
        self.assertEqual(0, next(it))
        self.assertEqual(1, next(it))
        self.assertEqual(2, next(it))
        with self.assertRaises(StopIteration):
            next(it)

    def test_peek_is_next(self) -> None:
        it = PeekingIterator(range(2))
        self.assertEqual(0, it.peek())
        self.assertTrue(it.has_more())
        self.assertEqual(0, next(it))
        self.assertTrue(it.has_more())
        self.assertEqual(1, next(it))
        self.assertFalse(it.has_more())
        with self.assertRaises(StopIteration):
            next(it)

    def test_peek_repeats(self) -> None:
        it = PeekingIterator(range(2))
        for _ in range(100):
            self.assertEqual(0, it.peek())
        self.assertEqual(0, next(it))
        self.assertEqual(1, next(it))

    def test_peek_after_exhaustion(self) -> None:
        it = PeekingIterator(range(2))
        self.assertEqual(0, next(it))
        self.assertEqual(1, next(it))
        with self.assertRaises(StopIteration):
            next(it)
        with self.assertRaises(StopIteration):
            it.peek()
        self.assertEqual(999, it.peek_default(999))

    def test_take_peeked(self) -> None:
        it = PeekingIterator(range(2))
        self.assertEqual(0, it.peek())
        it.take_peeked(0)
        self.assertEqual(1, next(it))
        with self.assertRaises(StopIteration):
            next(it)

    def test_take_peeked_wrong_value(self) -> None:
        it = PeekingIterator(range(2))
        self.assertEqual(0, it.peek())
        with self.assertRaisesRegex(AssertionError, 'expected the peaked value to be the same'):
            it.take_peeked(1)
        it.take_peeked(0)
        self.assertEqual(1, next(it))

# TODO: test PeekingAsyncIterator directly
