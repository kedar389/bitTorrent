import asyncio
import logging
import argparse

from bittorent_client.torrent_client import TorrentClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('torrent',
                        help='the .torrent to download')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='shows comunication between peers')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()

    torrent_client = TorrentClient(args.torrent)

    task = loop.create_task(torrent_client.start())
    loop.run_until_complete(task)


if __name__ == '__main__':
    main()
