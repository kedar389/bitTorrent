import asyncio
from torrent_client import TorrentClient
import logging



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    torrent_client = TorrentClient('C:\\Users\\RADEK_RYZEN\\Downloads\\ubuntu-22.04-desktop-amd64.iso.torrent')
    task = loop.create_task(torrent_client.start())

    loop.run_until_complete(task)


