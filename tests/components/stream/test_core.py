"""The tests for hls streams."""

import threading

from homeassistant.components.stream.core import BufferedStream


def _read_function(stream, success_container):
    count = 0
    success = True
    for current in stream.data():
        success = success and current == count
        count += 1
    success = success and count == 100
    success_container[0] = success_container[0] and success


def _write_function(stream):
    for i in range(0, 100):
        stream.write(i)


async def test_buffered_stream(hass, hass_client):
    """
    Test buffered stream.

    This will test the buffered stream's concurrency and make sure it's thread safe.
    """

    stream = BufferedStream()
    writethread = threading.Thread(
        name="writethread", target=_write_function, args=(stream,)
    )
    success_container = [True]
    read_threads = []
    for _ in range(10):
        thread = threading.Thread(
            name="read_thread", target=_read_function, args=(stream, success_container,)
        )
        thread.start()
        read_threads.append(thread)

    writethread.start()
    writethread.join()
    writethread = None
    stream.close()

    for read_thread in read_threads:
        read_thread.join()
        read_thread = None
    assert success_container[0]
