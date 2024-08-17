import os
import glob
import tarfile


class TarManager:

    def __init__(self, source_dir: set = "./", dest_dir: str = "./") -> None:
        self.source_dir = source_dir
        self.dest_dir = dest_dir

    def get_file_patterns_paths(self, patterns: set) -> set:
        file_paths = set()
        for pattern in patterns:
            matched_files = glob.glob(os.path.join(dir, pattern), recursive=True)
            file_paths.update(matched_files)
        return file_paths

    def tar(self, tar_name: str, file_paths: set) -> None:
        if self.dest_dir:
            os.makedirs(self.dest_dir, exist_ok=True)
            tar_name = os.path.join(self.dest_dir, tar_name)

        if not tar_name.endswith(".tar.gz"):
            tar_name += ".tar.gz"

        with tarfile.open(tar_name, "w:gz") as tar:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                tar.write(file_path, arcname=filename)

    def archive(self, tar_name: str, file_patterns: list) -> None:
        paths = self.get_file_patterns_paths(file_patterns)
        self.tar(self, tar_name, paths)

    def set_source_dir(self, dir: str) -> None:
        self.source_dir = dir

    def set_dest_dir(self, dest_dir: str) -> None:
        self.dest_dir = dest_dir
