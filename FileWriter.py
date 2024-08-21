class FileWriter:

    def __init__(self, data: list):
        self.data = data

    def write(self, file_name: str, dest_dir: str = "./") -> None:
        file_path = f"{dest_dir}/{file_name}"
        with open(file_path, "w") as file:
            for line in self.data:
                file.write(line + '\n')
