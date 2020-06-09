"""Provides core stream functionality."""
import asyncio
from collections import deque
import queue
import threading
from typing import Any, List

from aiohttp import web
import attr

from homeassistant.components.http import HomeAssistantView
from homeassistant.core import callback
from homeassistant.helpers.event import async_call_later
from homeassistant.util.decorator import Registry

from .const import ATTR_STREAMS, DOMAIN

PROVIDERS = Registry()


class QueueWrapper:
    """Wraps a thread safe FIFO queue."""

    def __init__(self, timeout=0.2, max_length=50):
        """Initializes the queue wrapper with timeout and max length."""
        self.queue = queue.Queue(max_length)
        self.queue_closed_event = threading.Event()
        self.timeout = timeout

    def put(self, data):
        """Will wait for an available spot on the queue and add the data."""
        self.queue.put(data)

    def try_put(self, data):
        """
        Best effor attempt to add data to the queue.

        Will attempt to add the data to the queue if there is a spot available.
        If the queue is full, will discard the first element and attempt to add the data.
        """
        try:
            # If the queue is full prioritize the new data, therefore remove an element
            if self.queue.full():
                try:
                    data = self.queue.get_nowait()
                    self.queue.task_done()
                except queue.Empty:
                    pass
            # This is a best effort attempt to put the element
            self.queue.put_nowait(data)
        except queue.Full:
            pass

    def get(self, mark_done=True):
        """
        Will wait for data on the queue and return it.

        If mark_done is set to False, will not mark the queue as ready for another read,
        and all other read operations will block until the queue is marked done.
        """
        while not self.queue.empty() or not self.queue_closed_event.is_set():
            try:
                # Basically we try to get data from the queue
                # if it's empty it will block for timeout seconds before throwing an exception
                data = self.queue.get(timeout=self.timeout)
                if mark_done:
                    self.mark_done()
                return data
            except queue.Empty:
                pass
        return None

    def mark_done(self):
        """Marks the queue as ready for a read operation."""
        self.queue.task_done()

    def close(self):
        """Closes the queue."""
        self.queue_closed_event.set()

    def is_closed(self):
        """Returns whether or not this queue is closed."""
        return self.queue_closed_event.is_set()


class BufferedStream:
    """A stream capable of buffering data being written to it."""

    def __init__(self):
        """Initializes the buffered stream."""
        self.queue = QueueWrapper(max_length=10000)
        self.saved_data = []

    def write(self, data):
        """Writes data to the stream."""
        self.queue.try_put(data)

    def data(self):
        """
        Returns stream as an iterator.

        Returns an iterator that will return all the data untill the stream is closed.
        This is call will block until the stream is closed.
        """
        index = 0
        while True:
            # This will block until there is available data or the queue has closed,
            # it will also block the gets on the queue until we release it.
            # If the data is None it means the queue has closed
            data = self.queue.get(mark_done=False)
            if data is not None:
                # We save the data and mark it as done to release the queue
                self.saved_data.append(data)
                self.queue.mark_done()
            data_length = len(self.saved_data)
            for i in range(index, data_length):
                yield self.saved_data[i]
            if data is None:
                break
            index = data_length

    def byte_array(self):
        """
        Returns the buffer data as a byte array.

        This is call will block until the stream is closed.
        """
        bytesdata = bytearray()
        for data in self.data():
            bytesdata.extend(data)
        return bytesdata

    def close(self):
        """Closes the stream."""
        self.queue.close()

    def is_closed(self):
        """Returns whether or not this stream is closed."""
        return self.queue.is_closed()


class SwitchableBufferedStream:
    """Buffered stream capable of switching its underlying buffer."""

    def __init__(self):
        """Initializes the switachable buffered stream."""
        self._current_stream = BufferedStream()

    def write(self, data):
        """Writes data to the stream."""
        self._current_stream.write(data)

    def get_current_stream(self):
        """Returns the underlying stream."""
        return self._current_stream

    def switch_stream(self):
        """Closes the current underlying stream and creates a new one."""
        # We simply replace the underlying stream
        # to write data somewhere else
        self._current_stream.close()
        self._current_stream = BufferedStream()
        return self._current_stream

    def close(self):
        """Closes the current underlying stream."""
        self._current_stream.close()


@attr.s
class StreamBuffer:
    """Represent a segment."""

    segment = attr.ib()  # type=SwitchableBufferedStream
    output = attr.ib()  # type=av.OutputContainer
    vstream = attr.ib()  # type=av.VideoStream
    astream = attr.ib(default=None)  # type=av.AudioStream
    requires_audio_decode = attr.ib(default=False)  # type=boolean
    frangible = attr.ib(default=False)  # type=boolean


@attr.s
class Segment:
    """Represent a segment."""

    sequence = attr.ib(type=int)
    segment = attr.ib(type=BufferedStream)
    duration = attr.ib(type=float)


class StreamOutput:
    """Represents a stream output."""

    def __init__(self, stream, timeout: int = 300) -> None:
        """Initialize a stream output."""
        self.idle = False
        self.max_segments = 6  # maximum number of queue segments
        self.num_segments = 3  # minimum number of segments returned by segments() call unless not enough available
        self.timeout = timeout
        self._stream = stream
        self._cursor = None
        self._event = asyncio.Event()
        self._segments = deque(maxlen=self.max_segments)
        self._unsub = None
        self._audio_stream = None

    @property
    def name(self) -> str:
        """Return provider name."""
        return None

    @property
    def format(self) -> str:
        """Return container format."""
        return None

    @property
    def audio_codec(self) -> str:
        """Return desired audio codec."""
        return None

    @property
    def video_codec(self) -> str:
        """Return desired video codec."""
        return None

    @property
    def frangible(self) -> str:
        """Returns wheather or not this stream can be fragmented mid-sequence."""
        # This is mainly to use the same output and simply switch the underlying buffer
        # on every sequence instead of creating an output per sequence.
        # Not all formats support this so it depends on the output.
        return False

    @property
    def segments(self) -> List[int]:
        """Return current sequence from segments."""
        elements = []
        min_count = self.num_segments
        current_length = len(self._segments)
        start_index = max(current_length - min_count, 0)
        for i in range(start_index, current_length):
            elements.append(self._segments[i].sequence)
        return elements

    @property
    def last_sequence(self) -> int:
        """Return the latest sequence from segments."""
        if len(self._segments) > 0:
            return self._segments[-1].sequence
        return 0

    @property
    def target_duration(self) -> int:
        """Return the maximum duration of the segments in seconds."""
        durations = [s.duration for s in self._segments]
        return round(max(durations)) or 1

    @property
    def preferred_audio_sample_rate(self) -> int:
        """Return the desired audio sample rate."""
        return None

    def should_wait(self, start_sequence):
        """Returns true if we should wait on the next sequence."""
        segments = self.segments
        if not segments:
            # There are simply no segments, you have to wait for one...
            return True
        if start_sequence + 1 <= self.last_sequence:
            # You are kind of behind...
            # This means you've either buffered on purpose because of bad latency, or
            # you've started this stream long ago.
            return False

        # If you fall here it means you have just started the stream for the first time.

        # Ok let's do something nice here...
        # There are 2 options each with a trad off
        # 1. Wait for the next sequence which will make the player wait until
        #    the next key frame. This will give the least latency between
        #    "real life" and the video. The problem though: Depending on the stream, we've seen
        #    cameras with as high as 5-8 seconds key frame interval, which means
        #    player may wait that long before video starts.
        # 2. Don't wait for the next sequence and just give it the currently available one.
        #    The problem: as mentioned earlier streams can have pretty high key frame intervals,
        #    this means we can be pretty far behind "real life".

        # Solution: dynamically decide between the two.
        # Basically let's check approximately how long is the wait time (the key frame interval).
        # If it's below 3 seconds I prefer to wait for the sequence to be near "real life".
        # If it's higher, then we don't want the player to timeout anyways so let's give it the current sequence.

        # We use the last segment duration as our key frame interval
        last_segment = self._segments[-1]
        if last_segment.duration > 3.0:
            return False
        return True

    @classmethod
    def is_audio_sample_rate_supported(cls, sample_rate):
        """Returns true if the sample rate is supported."""
        return False

    def get_segment(self, sequence: int = None) -> Any:
        """Retrieve a specific segment, or the whole list."""
        self.idle = False
        # Reset idle timeout
        if self._unsub is not None:
            self._unsub()
        self._unsub = async_call_later(self._stream.hass, self.timeout, self._timeout)

        if not sequence:
            return self._segments

        for segment in self._segments:
            if segment.sequence == sequence:
                return segment
        return None

    async def recv(self) -> Segment:
        """Wait for and retrieve the latest segment."""
        last_segment = max(self.segments, default=0)
        if self._cursor is None or self._cursor <= last_segment:
            await self._event.wait()

        if not self._segments:
            return None

        segment = self.get_segment()[-1]
        self._cursor = segment.sequence
        return segment

    @callback
    def put(self, segment: Segment) -> None:
        """Store output."""
        # Start idle timeout when we start receiving data
        if self._unsub is None:
            self._unsub = async_call_later(
                self._stream.hass, self.timeout, self._timeout
            )

        if segment is None:
            self._event.set()
            # Cleanup provider
            if self._unsub is not None:
                self._unsub()
            self.cleanup()
            return

        self._segments.append(segment)
        self._event.set()
        self._event.clear()

    @callback
    def _timeout(self, _now=None):
        """Handle stream timeout."""
        self._unsub = None
        if self._stream.keepalive:
            self.idle = True
            self._stream.check_idle()
        else:
            self.cleanup()

    def cleanup(self):
        """Handle cleanup."""
        self._segments = deque(maxlen=self.max_segments)
        self._stream.remove_provider(self)


class StreamView(HomeAssistantView):
    """
    Base StreamView.

    For implementation of a new stream format, define `url` and `name`
    attributes, and implement `handle` method in a child class.
    """

    requires_auth = False
    platform = None

    async def get(self, request, token, sequence=None):
        """Start a GET request."""
        hass = request.app["hass"]

        stream = next(
            (
                s
                for s in hass.data[DOMAIN][ATTR_STREAMS].values()
                if s.access_token == token
            ),
            None,
        )

        if not stream:
            raise web.HTTPNotFound()

        # Start worker if not already started
        stream.start()

        return await self.handle(request, stream, sequence)

    async def handle(self, request, stream, sequence):
        """Handle the stream request."""
        raise NotImplementedError()
