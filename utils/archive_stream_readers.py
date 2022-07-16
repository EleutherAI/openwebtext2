import lzma
import zstandard as zstd
import bz2

# Lets you access the underyling file's tell function for updating pqdm
class ArchiveStreamReader(object):
    def __init__(self, file_path, decompressor):
        self.file_path = file_path
        self.file_handle = None
        self.decompressor = decompressor
        self.stream_reader = None

    def __enter__(self):
        self.file_handle = open(self.file_path, 'rb')
        self.stream_reader = self.decompressor(self.file_handle)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stream_reader.close()        
        self.file_handle.close()

    def tell(self):
        return self.file_handle.tell()

    def read(self, size):
        return self.stream_reader.read(size)

def get_archive_stream_reader(file_path):
    extension = file_path.split(".")[-1]

    if extension == "zst":
        return ArchiveStreamReader(file_path, zstd.ZstdDecompressor(max_window_size=2147483648).stream_reader)
    elif extension == "bz2":
        return ArchiveStreamReader(file_path, bz2.BZ2File)
    elif extension == "xz":
        return ArchiveStreamReader(file_path, lzma.open)