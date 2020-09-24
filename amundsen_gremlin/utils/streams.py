# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import threading
from typing import (
    Any, AsyncIterator, Callable, Collection, Iterable, Iterator, List,
    Optional, Tuple, TypeVar, Union
)

from typing_extensions import Final, final

LOGGER = logging.getLogger(__name__)


V = TypeVar('V')
R = TypeVar('R')


def one(ignored: Any) -> int:
    return 1


class PeekingIterator(Iterator[V]):
    """
    Like Iterator, but with peek(), peek_default(), and take_peeked()
    """
    def __init__(self, iterable: Iterable[V]):
        self.it: Final[Iterator[V]] = iterable if isinstance(iterable, Iterator) else iter(iterable)
        self.has_peeked_value = False
        self.peeked_value: Optional[V] = None
        # RLock could make sense, but it would be just weird for the same thread to try to peek from same blocking
        # iterator
        self.lock: Final[threading.Lock] = threading.Lock()

    @final
    # @overrides Iterator but @overrides doesn't like
    def __next__(self) -> V:
        """
        :return: the previously peeked value or the next
        :raises StopIteration if there is no more values
        """
        with self.lock:
            value: V
            if self.has_peeked_value:
                value = self.peeked_value  # type: ignore
                self.peeked_value = None
                self.has_peeked_value = False
            else:
                value = next(self.it)
            assert not self.has_peeked_value
            return value

    @final
    def peek(self) -> V:
        """
        :return: the previously peeked value or the next
        :raises StopIteration if there is no more values
        """
        with self.lock:
            if not self.has_peeked_value:
                self.peeked_value = next(self.it)
                self.has_peeked_value = True
            assert self.has_peeked_value
            return self.peeked_value    # type: ignore

    @final
    def peek_default(self, default: Optional[V]) -> Optional[V]:
        """
        :return: the previously peeked value or the next, or default if no more values
        """
        try:
            return self.peek()
        except StopIteration:
            return default

    @final
    def take_peeked(self, value: V) -> None:
        with self.lock:
            assert self.has_peeked_value, f'expected to find a peaked value'
            assert self.peeked_value is value, f'expected the peaked value to be the same'
            self.peeked_value = None
            self.has_peeked_value = False

    @final
    def has_more(self) -> bool:
        try:
            self.peek()
            return True
        except StopIteration:
            return False


class PeekingAsyncIterator(AsyncIterator[V]):
    """
    Like AsyncIterator, but with peek(), peek_default(), and take_peeked()
    """
    def __init__(self, iterable: AsyncIterator[V]):
        self.it: Final[AsyncIterator[V]] = iterable
        self.has_peeked_value = False
        self.peeked_value: Optional[V] = None
        # RLock could make sense, but it would be just weird for the same thread to try to peek from same blocking
        # iterator
        self.lock: Final[threading.Lock] = threading.Lock()

    @final
    # @overrides AsyncIterator but @overrides doesn't like
    async def __anext__(self) -> V:
        """
        :return: the previously peeked value or the next
        :raises StopAsyncIteration if there is no more values
        """
        with self.lock:
            value: V
            if self.has_peeked_value:
                value = self.peeked_value  # type: ignore
                self.peeked_value = None
                self.has_peeked_value = False
            else:
                value = await self.__anext__()
            assert not self.has_peeked_value
            return value

    @final
    async def peek(self) -> V:
        """
        :return: the previously peeked value or the next
        :raises StopAsyncIteration if there is no more values
        """
        with self.lock:
            if not self.has_peeked_value:
                self.peeked_value = await self.it.__anext__()
                self.has_peeked_value = True
            assert self.has_peeked_value
            return self.peeked_value    # type: ignore

    @final
    async def peek_default(self, default: Optional[V]) -> Optional[V]:
        """
        :return: the previously peeked value or the next, or default if no more values
        """
        try:
            return await self.peek()
        except StopAsyncIteration:
            return default

    @final
    def take_peeked(self, value: V) -> None:
        with self.lock:
            assert self.has_peeked_value, f'expected to find a peaked value'
            assert self.peeked_value is value, f'expected the peaked value to be the same'
            self.peeked_value = None
            self.has_peeked_value = False

    @final
    async def has_more(self) -> bool:
        try:
            await self.peek()
            return True
        except StopAsyncIteration:
            return False


def one_chunk(*, it: PeekingIterator[V], n: int, metric: Callable[[V], int]) -> Tuple[Iterable[V], bool]:
    """
    :param it: stream of values as a PeekingIterator (or regular iterable if you are only going to take the first chunk
    and don't care about the peeked value being consumed)
    :param n: consume stream until n is reached.  if n is 0, process whole stream as one chunk.
    :param metric: the callable that returns positive metric for a value
    :returns the chunk
    """
    items: List[V] = []
    items_metric: int = 0
    try:
        while True:
            item = it.peek()
            item_metric = metric(item)
            # negative would be insane, let's say positive
            assert item_metric > 0, \
                f'expected metric to be positive! item_metric={item_metric}, metric={metric}, item={item}'
            if not items and item_metric > n:
                # should we assert instead? it's probably a surprise to the caller too, and might fail for whatever
                # limit they were trying to avoid, but let's give them a shot at least.
                LOGGER.error(f"expected a single item's metric to be less than the chunk limit! {item_metric} > {n}, "
                             f"but returning to make progress")
                items.append(item)
                it.take_peeked(item)
                items_metric += item_metric
                break
            elif items_metric + item_metric <= n:
                items.append(item)
                it.take_peeked(item)
                items_metric += item_metric
                if items_metric >= n:
                    # we're full
                    break
                # else keep accumulating
            else:
                assert items_metric + item_metric > n
                # we're full
                break
    # don't catch exception, let that be a concern for callers
    except StopIteration:
        pass

    has_more = it.has_more()
    return tuple(items), has_more


def chunk(it: Union[Iterable[V], PeekingIterator[V]], n: int, metric: Callable[[V], int] = one
          ) -> Iterable[Iterable[V]]:
    """
    :param it: stream of values as a PeekingIterator (or regular iterable if you are only going to take the first chunk
    and don't care about the peeked value being consumed)
    :param n: consume stream until n is reached.  if n is 0, process whole stream as one chunk.
    :param metric: the callable that returns positive metric for a value
    :returns the Iterable (generator) of chunks
    """
    if not isinstance(it, PeekingIterator):
        it = PeekingIterator(it)
    assert isinstance(it, PeekingIterator)
    has_more: bool = True
    while has_more:
        items, has_more = one_chunk(it=it, n=n, metric=metric)
        if items or has_more:
            yield items


async def async_one_chunk(
        it: PeekingAsyncIterator[V], n: int, metric: Callable[[V], int] = one) -> Tuple[Iterable[V], bool]:
    """
    :param it: stream of values as a PeekingAsyncIterator
    :param n: consume stream until n is reached.  if n is 0, process whole stream as one chunk.
    :param metric: the callable that returns positive metric for a value
    :returns the chunk and if there are more items
    """
    items: List[V] = []
    items_metric: int = 0
    if not isinstance(it, PeekingAsyncIterator):
        it = PeekingAsyncIterator(it)
    assert isinstance(it, PeekingAsyncIterator)
    try:
        while True:
            item = await it.peek()
            item_metric = metric(item)
            # negative would be insane, let's say positive
            assert item_metric > 0, \
                f'expected metric to be positive! item_metric={item_metric}, metric={metric}, item={item}'
            if not items and item_metric > n:
                # should we assert instead? it's probably a surprise to the caller too, and might fail for whatever
                # limit they were trying to avoid, but let's give them a shot at least.
                LOGGER.error(f"expected a single item's metric to be less than the chunk limit! {item_metric} > {n}, "
                             f"but returning to make progress")
                items.append(item)
                it.take_peeked(item)
                items_metric += item_metric
                break
            elif items_metric + item_metric <= n:
                items.append(item)
                it.take_peeked(item)
                items_metric += item_metric
                if items_metric >= n:
                    # we're full
                    break
                # else keep accumulating
            else:
                assert items_metric + item_metric > n
                # we're full
                break
    # don't catch exception, let that be a concern for callers
    except StopAsyncIteration:
        pass

    has_more = await it.has_more()
    return tuple(items), has_more


async def async_chunk(*, it: Union[AsyncIterator[V], PeekingAsyncIterator[V]], n: int, metric: Callable[[V], int]
                      ) -> AsyncIterator[Iterable[V]]:
    """
    :param it: stream of values as a PeekingAsyncIterator
    :param n: consume stream until n is reached.  if n is 0, process whole stream as one chunk.
    :param metric: the callable that returns positive metric for a value
    :returns the chunk and if there are more items
    """
    if not isinstance(it, PeekingAsyncIterator):
        it = PeekingAsyncIterator(it)
    assert isinstance(it, PeekingAsyncIterator)
    has_more: bool = True
    while has_more:
        items, has_more = await async_one_chunk(it=it, n=n, metric=metric)
        if items or has_more:
            yield items


def reduce_in_chunks(*, stream: Iterable[V], n: int, initial: R,
                     consumer: Callable[[Iterable[V], R], R], metric: Callable[[V], int] = one) -> R:
    """
    :param stream: stream of values
    :param n: consume stream until n is reached.  if n is 0, process whole stream as one chunk.
    :param metric: the callable that returns positive metric for a value
    :param initial: the initial state
    :param consumer: the callable to handle the chunk
    :returns the final state
    """
    if n > 0:
        it = PeekingIterator(stream)
        state = initial
        for items in chunk(it=it, n=n, metric=metric):
            state = consumer(items, state)
        return state
    else:
        return consumer(stream, initial)


async def async_reduce_in_chunks(*, stream: AsyncIterator[V], n: int, metric: Callable[[V], int], initial: R,
                                 consumer: Callable[[Iterable[V], R], R]) -> R:
    """
    :param stream:
    :param n: if n is 0, process whole stream as one chunk
    :param metric: the callable that returns positive metric for a value
    :param initial: the initial state
    :param consumer: the callable to handle the chunk
    :returns the final state
    """
    if n > 0:
        it = PeekingAsyncIterator(stream)
        state = initial
        async for items in async_chunk(it=it, n=n, metric=metric):
            state = consumer(items, state)
        return state
    else:
        return consumer(tuple([_ async for _ in stream]), initial)


def consume_in_chunks(*, stream: Iterable[V], n: int, consumer: Callable[[Iterable[V]], None],
                      metric: Callable[[V], int] = one) -> int:
    """
    :param stream:
    :param n: consume stream until n is reached if n is 0, process whole stream as one chunk
    :param metric: the callable that returns positive metric for a value
    :param consumer: the callable to handle the chunk
    :return:
    """
    _actual_state: int = 0

    def _consumer(things: Iterable[V], ignored: None) -> None:
        nonlocal _actual_state
        things = _assure_collection(things)
        assert isinstance(things, Collection)   # appease the types
        _actual_state += len(things)
        consumer(things)
    reduce_in_chunks(stream=stream, n=n, initial=None, consumer=_consumer, metric=metric)
    return _actual_state


# NB: This will not work on python 3.6; requires 3.7 or later
async def async_consume_in_chunks(*, stream: AsyncIterator[V], n: int, consumer: Callable[[Iterable[V]], None],
                                  metric: Callable[[V], int] = one) -> int:
    _actual_state: int = 0

    def _consumer(things: Iterable[V], ignored: None) -> None:
        nonlocal _actual_state
        things = _assure_collection(things)
        assert isinstance(things, Collection)   # appease the types
        _actual_state += len(things)
        consumer(things)
    await async_reduce_in_chunks(stream=stream, n=n, initial=None, consumer=_consumer, metric=metric)
    return _actual_state


def consume_in_chunks_with_state(*, stream: Iterable[V], n: int, consumer: Callable[[Iterable[V]], None],
                                 state: Callable[[V], R], metric: Callable[[V], int] = one) -> Iterable[R]:
    _actual_state: List[R] = list()

    def _consumer(things: Iterable[V], ignored: None) -> None:
        nonlocal _actual_state
        things = _assure_collection(things)
        assert isinstance(things, Collection)   # appease the types
        _actual_state.extend(map(state, things))
        consumer(things)

    reduce_in_chunks(stream=stream, n=n, initial=None, consumer=_consumer, metric=metric)
    return tuple(_actual_state)


def _assure_collection(iterable: Iterable[V]) -> Collection[V]:
    if isinstance(iterable, Collection):
        return iterable
    else:
        return tuple(iterable)
