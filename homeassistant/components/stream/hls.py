"""Provide functionality to stream HLS."""
import asyncio

from aiohttp import web

from homeassistant.core import callback
from homeassistant.util.dt import utcnow

from .const import AUDIO_SAMPLE_RATE, FORMAT_CONTENT_TYPE
from .core import PROVIDERS, StreamOutput, StreamView


@callback
def async_setup_hls(hass):
    """Set up api endpoints."""
    hass.http.register_view(HlsPlaylistView())
    hass.http.register_view(HlsSegmentView())
    return "/api/hls/{}/playlist.m3u8?start_sequence={}"


class HlsPlaylistView(StreamView):
    """Stream view to serve a M3U8 stream."""

    url = r"/api/hls/{token:[a-f0-9]+}/playlist.m3u8"
    name = "api:stream:hls:playlist"
    cors_allowed = True

    async def handle(self, request, stream, sequence):
        """Return m3u8 playlist."""
        renderer = M3U8Renderer(stream)
        track = stream.add_provider("hls")
        start_sequence = 0
        # If start sequence is specified use it to filter sequences
        if "start_sequence" in request.rel_url.query:
            start_sequence = int(request.rel_url.query["start_sequence"])
        stream.start()
        # Wait for a segment to be ready
        if track.should_wait(start_sequence):
            await track.recv()
        headers = {"Content-Type": FORMAT_CONTENT_TYPE["hls"]}
        return web.Response(
            body=renderer.render(track, start_sequence, utcnow()).encode("utf-8"),
            headers=headers,
        )


class HlsSegmentView(StreamView):
    """Stream view to serve a MPEG2TS segment."""

    url = r"/api/hls/{token:[a-f0-9]+}/segment/{sequence:\d+}.ts"
    name = "api:stream:hls:segment"
    cors_allowed = True

    async def handle(self, request, stream, sequence):
        """Return mpegts segment."""
        track = stream.add_provider("hls")
        segment = track.get_segment(int(sequence))
        if not segment:
            return web.HTTPNotFound()
        headers = {"Content-Type": "video/mp2t"}

        response = web.StreamResponse(headers=headers)
        await response.prepare(request)

        # This is where the magic happens
        # We send the data as it is ready, which means we can fill the buffer while the client is requesting it.
        # This gives minimal latency between the stream and "real life" since the player can start downloading
        # the data before we fully complete it.
        # Eventhough this helps, latency will still not be the best since hls.js doesn't support LHLS (low latency HLS) yet.
        # For more info on LHLS support in hls.js: https://github.com/video-dev/hls.js/pull/2370
        # The forked jwplayer of hls.js does support LHLS on one of the branches.
        for data in segment.segment.data():
            # This is important because we will never yield if the client can read as much as the server writes.
            # This is a known aiohttp design decision.
            # For more info see https://gitter.im/aio-libs/Lobby?at=5edade702c49c45f5ac61037
            await asyncio.sleep(0)
            await response.write(data)
        return response


class M3U8Renderer:
    """M3U8 Render Helper."""

    def __init__(self, stream):
        """Initialize renderer."""
        self.stream = stream

    @staticmethod
    def render_preamble(track):
        """Render preamble."""
        return ["#EXT-X-VERSION:3", f"#EXT-X-TARGETDURATION:{track.target_duration}"]

    @staticmethod
    def render_playlist(track, start_sequence, start_time):
        """Render playlist."""
        segments = track.segments

        if not segments:
            return []

        # Filter wanted sequences with the given start sequence
        valid_segments = [s for s in segments if s >= start_sequence]
        if not valid_segments:
            return []

        playlist = ["#EXT-X-MEDIA-SEQUENCE:{}".format(valid_segments[0])]

        for sequence in valid_segments:
            segment = track.get_segment(sequence)
            playlist.extend(
                [
                    "#EXTINF:{:.04f},".format(float(segment.duration)),
                    f"./segment/{segment.sequence}.ts",
                ]
            )

        return playlist

    def render(self, track, start_sequence, start_time):
        """Render M3U8 file."""
        lines = (
            ["#EXTM3U"]
            + self.render_preamble(track)
            + self.render_playlist(track, start_sequence, start_time)
        )
        return "\n".join(lines) + "\n"


@PROVIDERS.register("hls")
class HlsStreamOutput(StreamOutput):
    """Represents HLS Output formats."""

    @property
    def name(self) -> str:
        """Return provider name."""
        return "hls"

    @property
    def format(self) -> str:
        """Return container format."""
        return "mpegts"

    @property
    def audio_codec(self) -> str:
        """Return desired audio codec."""
        return "aac"

    @property
    def video_codec(self) -> str:
        """Return desired video codec."""
        return "h264"

    @property
    def preferred_audio_sample_rate(self) -> int:
        """Return the desired audio sample rate."""
        return AUDIO_SAMPLE_RATE

    @classmethod
    def is_audio_sample_rate_supported(cls, sample_rate):
        """Returns true if the sample rate is supported."""
        return sample_rate < 48000

    @property
    def frangible(self) -> str:
        """Returns wheather or not this stream can be fragmented mid-sequence."""
        # This is true for HLS since we use mpegts
        # so the output of every packet has the correct headers.
        return True
