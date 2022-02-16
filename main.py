import asyncio
import random

from torrent import Torrent
from tracker import Tracker







if __name__ == '__main__':
        torrent1 = Torrent('C:\\Users\\RADEK_RYZEN\\Downloads\\Partisans 1941 [FitGirl Repack].torrent')
        torrent2 = Torrent('C:\\Users\\RADEK_RYZEN\\Downloads\\ubuntu-21.10-desktop-amd64.iso.torrent')



        loop = asyncio.get_event_loop()
        tracker = Tracker(torrent2)
        loop.run_until_complete(loop.create_task(tracker.connect()))

        loop.run_until_complete(tracker.close_connection())








