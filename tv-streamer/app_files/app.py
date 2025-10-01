def mjpeg_generator(hls_url: str, width: int = 640, fps: int = 20, jpeg_quality: int = 6):
    """
    Spawn ffmpeg to pull HLS, transcode video to a stream of JPEG frames,
    and yield a well-formed multipart/x-mixed-replace stream.
    """
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-re",                        # real-time pacing
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-user_agent", "Mozilla/5.0",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
        "-i", hls_url,
        "-an",                        # no audio in MJPEG
        "-vf", f"fps={fps},scale={width}:-1",
        "-vcodec", "mjpeg",
        "-q:v", str(jpeg_quality),    # 2(best) .. 31(worst)
        "-f", "image2pipe",
        "-"
    ]
    # If your source needs a Referer, uncomment the next two args and set a value:
    # cmd[cmd.index("-user_agent"):cmd.index("-user_agent")+2] = ["-headers", "Referer: https://example.com\r\nUser-Agent: Mozilla/5.0"]

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, bufsize=0)

    boundary = b"--frame"
    # header ends with \r\n\r\n ; we will also add a trailing \r\n after the JPEG payload
    header_tmpl = b"\r\n".join([
        boundary,
        b"Content-Type: image/jpeg",
        b"Content-Length: %d",
        b"",
        b""
    ])

    buf = bytearray()
    try:
        while True:
            chunk = proc.stdout.read(4096)
            if not chunk:
                # ffmpeg ended; log stderr for diagnosis
                err = proc.stderr.read().decode("utf-8", "ignore").strip()
                if err:
                    app.logger.error("FFmpeg exited: %s", err)
                break
            buf.extend(chunk)
            # JPEG end-of-image marker 0xFF 0xD9
            while True:
                eoi = buf.find(b"\xff\xd9")
                if eoi == -1:
                    break
                frame = bytes(buf[:eoi + 2])
                del buf[:eoi + 2]
                # IMPORTANT: trailing CRLF after the JPEG
                yield header_tmpl % len(frame) + frame + b"\r\n"
    except GeneratorExit:
        pass
    except Exception as e:
        app.logger.exception("MJPEG generator error: %r", e)
    finally:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

@app.route("/mjpeg/<int:stream_id>")
def mjpeg(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    gen = mjpeg_generator(stream.hls_url, width=640, fps=20, jpeg_quality=6)
    return Response(
        gen,
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )