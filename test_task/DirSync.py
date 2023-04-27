from abc import ABC, abstractmethod
from pathlib import Path
import hashlib
import argparse


class AbstractFS(ABC):
    """
    Abstract class defines the interface of abstract file system.
    """

    @abstractmethod
    def get_files(self) -> list:
        pass

    @abstractmethod
    def get_file_name(self) -> str:
        pass

    @abstractmethod
    def rename_file(self, new_name: str):
        pass

    @abstractmethod
    def remove_file(self):
        pass

    @abstractmethod
    def open_file(self, mode):
        pass

    @abstractmethod
    def close_file(self):
        pass

    @abstractmethod
    def read_chunk(self, chunk_size: int) -> str:
        pass

    @abstractmethod
    def write_chunk(self, chunk: str):
        pass

    @abstractmethod
    def create_file(self, name: str):
        pass


class StandardFS(AbstractFS):
    """
    Implementation for standard file systems (like Windows, macOS). All methods are redefined using "pathlib".
    """

    def __init__(self, filepath=None):
        self.filepath = filepath
        self.file_obj = None

    def get_files(self) -> list:
        return [StandardFS(filepath) for filepath in Path(self.filepath).glob('*.*')]

    def get_file_name(self) -> str:
        return self.filepath.name

    def rename_file(self, new_name: str):
        self.filepath = self.filepath.rename(Path(self.filepath.parent, new_name))

    def remove_file(self):
        self.filepath.unlink()

    def open_file(self, mode='rb+'):
        self.file_obj = open(self.filepath, mode)

    def close_file(self):
        self.file_obj.close()
        self.file_obj = None

    def read_chunk(self, chunk_size) -> str:
        return self.file_obj.read(chunk_size)

    def write_chunk(self, chunk):
        self.file_obj.write(chunk)

    def create_file(self, name: str):
        new_file = Path(self.filepath, name)
        new_file.touch()
        return StandardFS(new_file)


def copy_file(source_file: AbstractFS, destination_file: AbstractFS):
    """The function copies the binary content from source_file to destination_file."""

    try:
        chunk_size = 4096
        source_file.open_file('rb')
        destination_file.open_file('ab')
        chunk = source_file.read_chunk(chunk_size)
        while len(chunk) > 0:
            destination_file.write_chunk(chunk)
            chunk = source_file.read_chunk(chunk_size)
    finally:
        source_file.close_file()
        destination_file.close_file()


def get_file_hash(file: AbstractFS) -> str:
    """""The function calculates the hash of the file."""""

    try:
        block_size = 65536
        file.open_file('rb')
        file_hash = hashlib.md5()
        chunk = file.read_chunk(block_size)
        while len(chunk) > 0:
            file_hash.update(chunk)
            chunk = file.read_chunk(block_size)
    finally:
        file.close_file()
    return file_hash.hexdigest()


def binary_matching(file1: AbstractFS, file2: AbstractFS) -> bool:
    """
    Binary comparing of two files.
    If files have equal binary content - return True, else - False.
    This function is necessary because hash matching cannot be used to assert
    that files have the same binary content.
    """

    chunk_size = 4096
    try:
        file1.open_file('rb')
        file2.open_file('rb')
        while True:
            data1 = file1.read_chunk(chunk_size)
            data2 = file2.read_chunk(chunk_size)
            if data1 != data2:
                file1.close_file()
                file2.close_file()
                return False
            if not data1:
                break
    finally:
        file1.close_file()
        file2.close_file()
    return True


def separate_files(dict_source: dict, destination_files: list) -> dict:
    """
    The function compares each file from destination_files with files from dict_source and do next operations:
    1. if file has equal hash, binary content and name - just remove this file from dict_source
    2. if file has equal hash, binary content and different name - rename file, appending "_for_rename" and
    add this file to the dict_files_for_rename dictionary with key = hash of this file.
    3. if there are no files in dict_source with equal hash or binary comparing returns False - remove the file
    """

    dict_files_for_rename = {}
    for file in destination_files:
        file_hash = get_file_hash(file)
        file_not_in_source = True
        equal_file = None
        if file_hash in dict_source:
            for source_file in dict_source[file_hash]:
                if binary_matching(file, source_file):
                    file_not_in_source = False
                    equal_file = source_file
                    if file.get_file_name() == source_file.get_file_name():
                        break
        if file_not_in_source:
            file.remove_file()
        else:
            if equal_file and file.get_file_name() != equal_file.get_file_name():
                file.rename_file(file.get_file_name() + '_for_rename')
                if file_hash in dict_files_for_rename:
                    dict_files_for_rename[file_hash].append(file)
                else:
                    dict_files_for_rename[file_hash] = [file]
            elif equal_file:
                dict_source[file_hash].remove(equal_file)
            if not dict_source[file_hash]:
                dict_source.pop(file_hash)
    return dict_files_for_rename


def rename_destination_files(dict_source, files_for_rename):
    """
    The function compares all files from files_for_rename with files from dict_source.
    If file has equal file - the function renames it and removes equal file from dict_source, else - delete.
    """

    for file_hash in files_for_rename:
        if file_hash in dict_source:
            for file_for_rename in files_for_rename[file_hash]:
                delete_file = None
                if not dict_source[file_hash]:
                    file_for_rename.remove_file()
                for source_file in dict_source[file_hash]:
                    if binary_matching(file_for_rename, source_file):
                        file_for_rename.rename_file(source_file.get_file_name())
                        delete_file = source_file
                        break
                if dict_source[file_hash]:
                    dict_source[file_hash].remove(delete_file)
            if not dict_source[file_hash]:
                dict_source.pop(file_hash)
        else:
            for file_for_rename in files_for_rename[file_hash]:
                file_for_rename.remove_file()


def copy_files_to_destination(dict_source, file_system: AbstractFS):
    """
    The function copies all files from dict_source to destination directory.
    """

    for file_hash in dict_source:
        for file in dict_source[file_hash]:
            new_file = file_system.create_file(file.get_file_name())
            copy_file(file, new_file)


def create_filehash_dictionary(files: list) -> dict:
    """
    Create dictionary containing file_hash and list of file objects which have equal hash.
    """

    dict_files = {}
    for file in files:
        file_hash = get_file_hash(file)
        if file_hash in dict_files:
            dict_files[file_hash].append(file)
        else:
            dict_files[file_hash] = [file]
    return dict_files


class DirectorySynchronizer:

    def __init__(self, source_fs: AbstractFS, destination_fs: AbstractFS):
        self.source_fs = source_fs
        self.destination_fs = destination_fs

    def sync_directories(self):
        """
        The function synchronizes two directories: source and destination.
        If a file exists in the source but not in the destination - copies the file over.
        If a file exists in the source, but has a different name than in the destination -
        rename the destination file to match.
        If a file exists in the destination but not in the source - remove it.
        """

        source_files = self.source_fs.get_files()
        destination_files = self.destination_fs.get_files()

        dict_source_files = {}
        files_for_rename = None

        if source_files:
            dict_source_files = create_filehash_dictionary(source_files)
        if destination_files:
            files_for_rename = separate_files(dict_source_files, destination_files)
        if files_for_rename:
            rename_destination_files(dict_source_files, files_for_rename)
        if dict_source_files:
            copy_files_to_destination(dict_source_files, self.destination_fs)


parser = argparse.ArgumentParser(description='Synchronizer for two directories.')
parser.add_argument('source_path', help='Absolute path to source directory.', type=str)
parser.add_argument('destination_path', help='Absolute path to destination directory.', type=str)
# Optional arguments allow you to use specific implementations of abstract file system. By default,
# you will use StandardFS.
parser.add_argument('-fs1', '--file_system1', type=int, default=0, choices=[0],
                    help='Type of file system implementation for source directory. Default 0: StandardFS')
parser.add_argument('-fs2', '--file_system2', type=int, default=0, choices=[0],
                    help='Type of file system implementation for destination directory. Default 0: StandardFS')

implementations = {0: StandardFS}  # If you create your own implementation, you need to add it here.

if __name__ == '__main__':
    args = parser.parse_args()
    source_sync = implementations[args.file_system1](args.source_path)
    destination_sync = implementations[args.file_system2](args.destination_path)
    synchronizer = DirectorySynchronizer(source_sync, destination_sync)
    synchronizer.sync_directories()
