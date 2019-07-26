# -*- coding: utf-8 -*-
import time
import subprocess

from tornado.concurrent import return_future

from thumbor.utils import logger
from thumbor.loaders import LoaderResult
from thumbor.loaders.http_loader import _normalize_url


def ffmpeg(context, uri, **flags):
    # TODO: Use tornado's subprocess
    def flags():
        user_agent = None

        if context.config.HTTP_LOADER_PROXY_HOST and context.config.HTTP_LOADER_PROXY_PORT:
            yield 'http_proxy', f'{context.config.HTTP_LOADER_PROXY_HOST}:{context.config.HTTP_LOADER_PROXY_PORT}'

        def headers():
            nonlocal user_agent

            if not context.request_handler:
                return

            if context.config.HTTP_LOADER_FORWARD_ALL_HEADERS:
                user_agent = context.request_handler.request.headers.get('User-Agent')
                yield from context.request_handler.request.headers.items()
                return

            whitelisted_headers = set(
                context.config.HTTP_LOADER_FORWARD_HEADERS_WHITELIST or []
            ).intersection(context.request_handler.request.headers)

            for header_key in whitelisted_headers:
                yield header_key, context.request_handler.request.headers[header_key]

        headers_string = '\r\n'.join([
            f'{header}: {value}' for header, value in headers()
        ])
        if headers_string:
            yield 'headers', headers_string

        # Ensure a User-Agent
        if not user_agent:
            if context.config.HTTP_LOADER_FORWARD_USER_AGENT:
                user_agent = context.request_handler.request.headers.get('User-Agent')
            yield 'user_agent', user_agent or context.config.HTTP_LOADER_DEFAULT_USER_AGENT

        yield 'timeout', str(max(
            context.config.HTTP_LOADER_CONNECT_TIMEOUT,
            context.config.HTTP_LOADER_REQUEST_TIMEOUT,
        ) * 1000000)

    def cmd():
        yield context.config.FFMPEG_PATH
        yield from ('-loglevel', 'fatal')
        yield '-nostats'

        for flag, value in flags():
            yield f'-{flag}'
            yield value

        yield from ('-i', uri)
        yield from ('-filter:v', "select='eq(pict_type,PICT_TYPE_I)'", '-vsync', 'vfr')
        yield from ('-frames:v', '1')
        yield from ('-c:v', 'mjpeg')
        yield from ('-qscale:v', '1')
        yield from ('-f', 'image2pipe', 'pipe:1')

    return subprocess.check_output(list(cmd()), stderr=subprocess.PIPE)


@return_future
def load(context, url, callback, normalize_url_func=_normalize_url):
    result = LoaderResult()
    start = time.perf_counter()

    try:
        result.buffer = ffmpeg(context, normalize_url_func(url))
    except subprocess.CalledProcessError as err:
        result.successful = False
        result.error = err.stderr.decode('utf-8').strip()

        logger.warn(f'ERROR retrieving image {url}: {result.error}')
        if result.error.lower().endswith('Server returned 404 not found'.lower()):
            result.error = LoaderResult.ERROR_NOT_FOUND
    except Exception as err:
        result.successful = False
        result.error = str(err)
        logger.warn(f'ERROR retrieving image {url}: {err}')
    else:
        total_time = (time.perf_counter() - start)
        total_bytes = len(result.buffer)

        result.metadata.update({
            'size': total_bytes,
            # 'updated_at': datetime.datetime.utcnow(),
        })

        context.metrics.incr('original_image.status.200')
        context.metrics.incr('original_image.response_bytes', total_bytes)
        context.metrics.timing(f'original_image.fetch.{url}', total_time * 1000)
        context.metrics.timing('original_image.time_info.bytes_per_second', total_bytes/total_time)

    return callback(result)


if __name__ == '__main__':
    from thumbor.config import Config
    from thumbor.context import Context

    ctx = Context(config=Config(
        FFMPEG_PATH = '/usr/bin/ffmpeg',
    ))

    result = load(ctx, 'http://b3723ca4.ap.ngrok.io/cliep.mp4', lambda x: x).result()
    # print(result)
    print(result.successful)
    print(result.error)
    # print(result.buffer)

    if result.successful:
        with open('thumbnail.jpg', 'wb') as fp:
            fp.write(result.buffer.read())
