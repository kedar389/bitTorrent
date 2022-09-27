import os
from pathlib import Path


class FileManager:
    def __init__(self, torrent):
        self.torrent = torrent
        self.loaded_pieces = self.load_downloaded_pieces()

    def _find_file_to_index(self, piece_index):
        """
        :param piece_index:
        :return: Returns file to which piece belongs and its offset in file
        """
        absolute_pos = piece_index * self.torrent.piece_length

        size_counter = 0
        file_index = 0

        # Find out to which file does piece belong
        for index, file in enumerate(self.torrent.files):
            # If size  of all previous files plus next file is bigger then abs pos  ,piece starts in this file
            if size_counter + file.length > absolute_pos:
                file_index = index
                break
            size_counter += file.length

        # Find position in file
        file_pos = absolute_pos - size_counter

        return (file_index, file_pos)

    def load_piece(self, piece_index):
        file_index, file_pos = self._find_file_to_index(piece_index)
        fd = os.open(self.torrent.files[file_index].name, os.O_RDWR | os.O_CREAT)
        os.lseek(fd, file_pos, os.SEEK_SET)
        return os.read(fd, self.torrent.piece_length)

    def write_piece(self, data, piece_index):
        """
        Write the given piece to disk
        """
        file_index, file_pos = self._find_file_to_index(piece_index)

        while len(data) > 0:
            output_file = Path(self.torrent.files[file_index].name)
            output_file.parent.mkdir(exist_ok=True, parents=True)
            fd = os.open(self.torrent.files[file_index].name, os.O_RDWR | os.O_CREAT)
            os.lseek(fd, file_pos, os.SEEK_SET)
            os.write(fd, data[:self.torrent.files[file_index].length - file_pos - 1])
            os.close(fd)

            data = data[self.torrent.files[file_index].length - file_pos:]
            file_pos = 0
            file_index += 1

    #TODO
    def load_downloaded_pieces(self):
        pieces = []
        for file in self.torrent.files:
            if Path(file.name).is_file():
                with open(file.name, 'rb') as file:
                    while True:
                        piece = file.read(self.torrent.piece_length)
                        if not piece:
                            break

        return pieces
