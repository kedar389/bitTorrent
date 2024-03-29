from bittorent_client.bencoding import Encoder, Decoder
from hashlib import sha1
from collections import namedtuple
import os

TorrentFile = namedtuple("TorrentFile", ['name', 'length'])


class Torrent:
    """
        Wrapper class around Decoder,
        This class contains info about torrent
    """

    def __init__(self, filepath):
        self.filepath = filepath
        self.files = []

        with open(filepath, 'rb') as file:
            meta_info = file.read()
            self._torrent_meta_info = Decoder(meta_info).decode()

        self._torrent_name = self._torrent_meta_info[b'info'][b'name'].decode('utf-8')

        # Tracker needs info part of torrent meta file as SHA1 hash,
        # 1.Bencode it 2.Hash it
        info = Encoder(self._torrent_meta_info[b'info']).encode()
        self.info_hash = sha1(info).digest()

        self._identify_files_to_download()
        self.size = self._size()

    @property
    def is_multifile(self):
        return b'files' in self._torrent_meta_info[b'info']

    def _identify_files_to_download(self):
        """
        Finds all files that are to be downloaded
        """
        if self.is_multifile:
            for file in self._torrent_meta_info[b'info'][b'files']:
                file_parts = [part.decode('utf-8') for part in file[b'path']]

                self.files.append(TorrentFile(
                    os.path.join(self._torrent_name, *file_parts),
                    file[b'length']
                ))
        else:
            self.files.append(
                TorrentFile(
                    self._torrent_name,
                    self._torrent_meta_info[b'info'][b'length']))

    def _size(self):
        if self.is_multifile:
            return sum(file[b'length'] for file in self._torrent_meta_info[b'info'][b'files'])

        return self._torrent_meta_info[b'info'][b'length']

    @property
    def pieces(self):
        # Every piece is  20 bytes long
        pieces_sha1_tring = self._torrent_meta_info[b'info'][b'pieces']
        pieces = [pieces_sha1_tring[i:i + 20] for i in range(0, len(pieces_sha1_tring), 20)]

        return pieces

    @property
    def piece_length(self):
        return self._torrent_meta_info[b'info'][b'piece length']

    # returns Tracker URL
    @property
    def announce(self):
        return self._torrent_meta_info[b'announce'].decode('utf-8')

    @property
    def announce_list(self):
        announce_list = []
        for announce in self._torrent_meta_info[b'announce-list']:
            announce_list.append(str(announce[0], 'utf-8'))
        return announce_list

    def __str__(self):
        if self.is_multifile:
            # TODO Add support for multi-file torrents
            pass

        else:
            return 'Filename {0} \n' + 'Size {1}' + 'Announce URL {2}'.format(self.files[0].name, self.size,
                                                                              self.announce)
