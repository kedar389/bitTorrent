import asyncio
from torrent_client import TorrentClient


#TODO Figure out how to accept connections from peers when you are finished with downloading, or if tracker gives u them
#TODO how to seed when you are finished , if you should break in torrent_client .


def main():
    loop = asyncio.get_event_loop()
    torrent_client = TorrentClient('C:\\Users\\RADEK_RYZEN\\Downloads\\ubuntu-21.10-desktop-amd64.iso.torrent')
    task = loop.create_task(torrent_client.start())
