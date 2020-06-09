"""Provides the worker thread needed for processing streams."""
import logging

import av

from .core import Segment, StreamBuffer, SwitchableBufferedStream

_LOGGER = logging.getLogger(__name__)


def should_decode(audio_stream, stream_output):
    """Returns whether or not we should decode the audio stream."""
    return (
        not audio_stream.name == stream_output.audio_codec
        or not stream_output.is_audio_sample_rate_supported(audio_stream.rate)
    )


def create_stream_buffer(stream_output, video_stream, audio_stream):
    """Create a new StreamBuffer."""

    segment = SwitchableBufferedStream()
    output = av.open(segment, mode="w", format=stream_output.format)
    astream = None
    requires_audio_decode = False
    vstream = output.add_stream(template=video_stream)
    # Check if audio is requested
    if (
        audio_stream is not None
        and stream_output.audio_codec is not None
        and stream_output.preferred_audio_sample_rate is not None
    ):
        if should_decode(audio_stream, stream_output):
            astream = output.add_stream(
                codec_name=stream_output.audio_codec,
                rate=stream_output.preferred_audio_sample_rate,
            )
            requires_audio_decode = True
        else:
            astream = output.add_stream(template=audio_stream)
    return StreamBuffer(
        segment,
        output,
        vstream,
        astream,
        requires_audio_decode,
        stream_output.frangible,
    )


def update_outputs(
    hass, outputs, stream, sequence, video_stream, audio_stream, target_duration
):
    """Updates the stream outputs."""
    # To prevent dict modified during iteration
    stream_output_keys = set(stream.outputs.keys())
    for fmt in stream_output_keys:
        stream_output = stream.outputs[fmt]
        current_buffer = outputs.get(fmt)
        create_buffer = True
        if current_buffer is not None:
            if current_buffer.frangible:
                # Since the output supports simply switching buffers then
                # we can simply switch the underlying buffer and continue writting
                create_buffer = False
                current_buffer.segment.switch_stream()
            else:
                # Close the buffer
                current_buffer.output.close()
                current_buffer.segment.close()

        if create_buffer:
            if video_stream.name != stream_output.video_codec:
                continue
            current_buffer = create_stream_buffer(
                stream_output, video_stream, audio_stream
            )
            outputs[stream_output.name] = current_buffer
        # Save segment to outputs
        hass.loop.call_soon_threadsafe(
            stream_output.put,
            Segment(
                sequence, current_buffer.segment.get_current_stream(), target_duration
            ),
        )

    # Let's get the outputs that are no longer available and close/remove them
    outputs_to_remove = set(outputs.keys()) - stream_output_keys
    for fmt in outputs_to_remove:
        outputs[fmt].output.close()
        outputs[fmt].segment.close()
        del outputs[fmt]


def handle_audio_decode(
    buffer, audio_frames, recalculate_audio_pts, starting_pts, time_base
):
    """Handle decoding the audio."""
    for a_frame in audio_frames:
        # Let avcodec decide of the pts
        a_frame.pts = None
        for a_packet in buffer.astream.encode(a_frame):
            a_packet.stream = buffer.astream
            # Let the avcodec decide of the next pts/dts
            # By default it increments by duration which is what we want
            a_packet.pts = None
            a_packet.dts = None
            if recalculate_audio_pts:
                # We will attempt to recalculate the audio pts/dts.
                # This will always happen on the first packet.
                audio_pts = int(starting_pts / a_packet.time_base * time_base)
                recalculate_audio_pts = False
                a_packet.pts = audio_pts
                a_packet.dts = audio_pts
            buffer.output.mux(a_packet)


def stream_worker_writer(
    hass, container, outputs, stream, quit_event, video_stream, audio_stream
):
    """Handle consuming streams."""

    # Keep track of the number of segments we've processed
    sequence = 1

    # -----Video Section------
    first_video_packet = True
    # The presentation timestamp of the first video packet we receive
    first_video_pts = 0
    # The decoder timestamp of the latest video packet we processed
    last_video_dts = None

    # -----Audio Section------
    first_audio_packet = True
    # The presentation timestamp of the first audio packet we receive
    first_audio_pts = 0
    # The expect audio pts value
    expected_audio_pts = None

    while not quit_event.is_set():
        packet = None
        if audio_stream is not None:
            packet = next(container.demux(video_stream, audio_stream))
        else:
            packet = next(container.demux(video_stream))

        if packet is None:
            raise StopIteration("Received none packet")

        if packet.stream.type == "video":
            if packet.dts is None:
                if first_video_packet:
                    continue
                # If we get a "flushing" packet, the stream is done
                raise StopIteration("No dts in packet")
            if last_video_dts is not None and last_video_dts >= packet.dts:
                continue
            last_video_dts = packet.dts
            if first_video_packet:
                first_video_pts = packet.pts
                first_video_packet = False

            # Reset timestamps from a 0 time base for this video stream
            packet.pts -= first_video_pts
            packet.dts -= first_video_pts

            if packet.is_keyframe:
                # Calculate the segment duration by multiplying the presentation
                # timestamp by the time base, which gets us total seconds.
                # By then dividing by the sequence, we can calculate how long
                # each segment is, assuming the stream starts from 0.
                # Here we use 0.5 seconds as a fallback for the first segment.
                segment_duration = (
                    float((packet.pts * packet.time_base) / sequence) or 0.5
                )
                update_outputs(
                    hass,
                    outputs,
                    stream,
                    sequence,
                    video_stream,
                    audio_stream,
                    segment_duration,
                )
                sequence += 1

            for buffer in outputs.values():
                if buffer.vstream:
                    packet.stream = buffer.vstream
                    buffer.output.mux(packet)
        elif packet.stream.type == "audio":
            if packet.dts is None:
                if first_audio_packet:
                    continue
                # If we get a "flushing" packet, the stream is done
                raise StopIteration("No dts in packet")
            if first_video_packet:
                # No need to send audio if we haven't received any video frames yet
                continue
            if first_audio_packet:
                # Since we use the first video pts as our zero value then we need
                # to calculate the audio starting pts using the video starting pts.
                first_audio_pts = int(
                    first_video_pts / packet.time_base * video_stream.time_base
                )
                first_audio_packet = False

            # Reset timestamps from a 0 time base for this audio stream
            packet.pts -= first_audio_pts
            packet.dts -= first_audio_pts

            # If we don't know what the expected pts is we need to calculate it
            recalculate_pts = expected_audio_pts is None

            if expected_audio_pts is not None and packet.pts != expected_audio_pts:
                # If the pts isn't what we expected it's either ahead or behind the current stream
                if expected_audio_pts < packet.pts:
                    # Probably lost some audio packets or there is an audio gap
                    # Let's recalculate our pts
                    recalculate_pts = True
                else:
                    # Just drop that thing... makes no sense to go backwards for audio
                    continue

            # set our next expected pts
            expected_audio_pts = packet.pts + packet.duration
            audio_frames = None
            for buffer in outputs.values():

                # If the output doesn't support audio let's skip it
                if buffer.astream is None:
                    continue

                # The output supports the incoming audio steram directly, no need to re-encode :)
                if not buffer.requires_audio_decode:
                    packet.stream = buffer.astream
                    buffer.output.mux(packet)
                    continue

                # If the output is receiving the first ever audio frame we need to calculate the audio pts
                recalculate_audio_pts = recalculate_pts or buffer.astream.frames == 0
                if audio_frames is None:
                    # Decode it once for all the outputs so that we don't do it on every iteration
                    audio_frames = packet.decode()
                handle_audio_decode(
                    buffer,
                    audio_frames,
                    recalculate_audio_pts,
                    packet.pts,
                    packet.time_base,
                )


def stream_worker(hass, stream, quit_event):
    """Handle consuming streams."""

    container = av.open(stream.source, options=stream.options)
    audio_stream = None
    video_stream = None
    try:
        video_stream = container.streams.video[0]
    except (KeyError, IndexError):
        _LOGGER.error("Stream has no video")
        return

    try:
        audio_stream = container.streams.audio[0]
    except (KeyError, IndexError):
        _LOGGER.info("Stream has no audio")

    # Holds the buffers for each stream provider
    outputs = {}
    try:
        stream_worker_writer(
            hass, container, outputs, stream, quit_event, video_stream, audio_stream
        )
    except (StopIteration, av.AVError) as ex:
        _LOGGER.error("Error demuxing stream: %s", str(ex))
    finally:
        # Let's close everything
        for buffer in outputs.values():
            buffer.output.close()
            buffer.segment.close()
        stream_output_keys = set(stream.outputs.keys())
        for key in stream_output_keys:
            hass.loop.call_soon_threadsafe(stream.outputs[key].put, None)
        container.close()
