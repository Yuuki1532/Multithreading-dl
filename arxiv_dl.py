import sys, time
import asyncio
from pathlib import Path
from collections import namedtuple
import aiohttp
# import aiofiles

max_chunk_size = 1 * 2 ** 20 # 1MB
max_connections_per_download = 20
# http header
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.113 Safari/537.36'
timeout = 30
save_dir = Path('.')
# define data structure for worker
QueuePayload = namedtuple('QueuePayload', ('session', 'url', 'byte_range', 'save_path'))

async def worker(*args, **kwargs):
    worker_index, chunk_queue = args

    while True:
        # get queue payload
        queue_payload = await chunk_queue.get()

        if queue_payload is None: # terminate
            chunk_queue.task_done()
            print(f'Terminating worker {worker_index}')
            return

        # unpack queue payload
        session, url, (start_byte, end_byte), save_path = queue_payload
        print(f'Worker {worker_index} got range: {start_byte} - {end_byte}')

        # get
        async with session.get(
            url,
            allow_redirects=False,
            headers={
                'User-agent': user_agent,
                'Range': f'bytes={start_byte}-{end_byte}',
            },
        ) as response:

            # not HTTP 2xx
            if not 200 <= response.status < 300:
                 raise aiohttp.ClientResponseError(status=response.status)

            # read response content
            content = await response.read()

            # check size
            bytes_downloaded = len(content)
            bytes_expected = end_byte - start_byte + 1
            if bytes_downloaded != bytes_expected:
                raise Exception(f'Downloaded {bytes_downloaded} bytes while {bytes_expected} bytes expected.')

            # write to file
            with open(save_path, 'r+b') as f:
                f.seek(start_byte)
                f.write(content)

            pass

        chunk_queue.task_done()
        pass

    pass

def iter_byte_range(file_size, max_chunk_size):
    start = 0
    while start < file_size - 1:
        end = min(start + max_chunk_size - 1, file_size - 1)
        yield start, end
        start = end + 1

async def download(url, max_connections):
    # create chunks queue
    chunk_queue = asyncio.Queue()

    # create workers
    workers = []
    for i in range(max_connections):
        task = asyncio.create_task(worker(i, chunk_queue))
        workers.append(task)
    print(f'Created {max_connections} workers')

    # create session
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:

        # head
        async with session.head(url, allow_redirects=True, headers={'User-agent': user_agent}) as response:

            # get redirected url
            url = response.url

            # assert byte ranges are supported
            assert response.headers['Accept-Ranges'] == 'bytes'

            # get file size
            file_size = int(response.headers['Content-Length'])

            # setup save filename
            save_path = save_dir / response.url.name
        print(f'Redirected to url: {url}')
        print(f'File size: {file_size}')
        print(f'Save to: {save_path}')

        # create tmp file for the download
        tmp_save_path = Path(f'{str(save_path)}.part')

        with open(tmp_save_path, 'x') as f:
            f.truncate(file_size)

        # split chunks for workers to download
        for byte_range in iter_byte_range(file_size, max_chunk_size):
            chunk_queue.put_nowait(QueuePayload(session, url, byte_range, tmp_save_path))

        # use None as signal to terminate
        for i in range(len(workers)):
            chunk_queue.put_nowait(None)

        # calculate time
        start_time = time.monotonic()

        # wait until the queue is fully processed, i.e. all chunks downloaded
        # await chunk_queue.join()

        # wait until all workers are terminated
        await asyncio.gather(*workers)
        print(f'All workers done')

        # queue should be empty
        assert chunk_queue.empty()

        # calculate time
        time_spent = time.monotonic() - start_time
        print(f'Downloaded in {time_spent:.2f} s')
        print(f'Average speed: {file_size / time_spent / 2 ** 10:.2f} KB/s')

        # rename tmp file
        tmp_save_path.rename(save_path)
        print(f'Renamed tmp file')

        pass


    pass

async def main(url):
    print(f'Download {url}')
    await download(url, max_connections_per_download)
    return 0

if __name__ == '__main__':
    # Usage: python arxiv_speedup.py PDF_URL
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # for Windows
    sys.exit(asyncio.run(main(sys.argv[1])))
