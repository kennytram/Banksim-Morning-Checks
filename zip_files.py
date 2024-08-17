import os
import zipfile


def get_file_paths(directory):
    file_paths = []
    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)
    return file_paths


def zip_files(file_paths, zip_name):
    with zipfile.ZipFile(zip_name, "w") as zip:
        for file_path in file_paths:
            zip.write(file_path)


directory = "./blobmount"
file_paths = get_file_paths(directory)
zip_files(file_paths, "blobmount.zip")
