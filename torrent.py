from bencoding import Encoder, Decoder
from hashlib import sha1
from collections import namedtuple

TorrentFile = namedtuple("TorrentFile", ['name', 'length'])

'''Wrapper class around Decoder,
 This class contains info about torrent file in structure of OrderedDictionary'''


class Torrent:

    def __init__(self, filepath):
        self.filepath = filepath
        self.files = []

        with open(filepath, 'rb') as file:
            meta_info = file.read()
            self._torrent_meta_info = Decoder(meta_info).decode()

            '''trackers needs info part of torrent meta file as SHA1 hash, 
            so you encode the info part back to bencode and then hash it to SHA1 '''
            info = Encoder(self._torrent_meta_info[b'info']).encode()

            self.info_hash = sha1(info).digest()
            self._identify_files_to_download()

    def _identify_files_to_download(self):
        """
        Finds all files that are to be downloaded
        """

        if self.is_multifile:
            # TODO Add support for multi-file torrents
            pass

        else:
            self.files.append(
                TorrentFile(
                    self._torrent_meta_info[b'info'][b'name'].decode('utf-8'),
                    self._torrent_meta_info[b'info'][b'length']))

    @property
    def is_multifile(self):
        return b'files' in self._torrent_meta_info[b'info']

    "returns Tracker URL"
    @property
    def announce(self):
        return self._torrent_meta_info[b'announce'].decode('utf-8')

    @property
    def pieces(self):
        '''Every piece is long 20 bytes'''
        pieces_sha1_tring = self._torrent_meta_info[b'info'][b'pieces']
        pieces = [ pieces_sha1_tring[i:i + 20] for i in range(0, len(pieces_sha1_tring), 20)]

        return pieces

    @property
    def piece_length(self):
        return self._torrent_meta_info[b'info'][b'piece length']

    @property
    def torrent_name(self):
        if self.is_multifile:
            # TODO Add support for multi-file torrents
            pass

        else:
            return self._torrent_meta_info[b'info'][b'name'].decode('utf-8')

    @property
    def torrent_size(self):
        if self.is_multifile:
            # TODO Add support for multi-file torrents
            pass

        else:
            return self._torrent_meta_info[b'info'][b'length']

    def __str__(self):
        if self.is_multifile:
            # TODO Add support for multi-file torrents
            pass

        else:
            return 'Filename {0} \n' + 'Size {1}' + 'Announce URL {2}'.format(self.torrent_name, self.torrent_size,
                                                                              self.announce)
