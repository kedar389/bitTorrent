# bitTorrent - Torrent CLI tool written in python

Whole client is written in python using asyncio.

Client adheres to these BEPS:

- The BitTorrent Protocol Specification (BEP 0003 ).

- Multitracker Metadata Extension (BEP 0012).

- Scrape peers from UDP or HTTP trackers (BEP 15).

- Tracker Returns Compact Peer Lists (BEP 0023).



What needs fixing:
- Loading partially downloaded files is currently broken(Will start from start)

- Cannot load torrent through magnet links nor get peers from DHT.

- Memory tweaking to have some pieces loaded in memory (does not hold any pieces).

- Implement rare first piece algorithm.

- Only one torrent per instance.


To install requirements:

pip install -r requirements.txt



Running the program

- Run: python client.py /path/to/your/file.torrent

- The files will be downloaded in the same path as your client.py script.
