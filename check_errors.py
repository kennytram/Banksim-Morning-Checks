import os

def check_file_exists(file_path):
    return os.path.isfile(file_path)

def check_for_errors(file_path):
    errors = list()
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            for line in lines:
                index = -1
                words = line.split()
                if 'ERROR' in line:
                    index = words.index('ERROR')
                if 'CRITICAL' in line:
                    index = words.index('CRITICAL')
                if index >= 0:
                    error_description = " ".join(words[index:])
                    errors.append(error_description)
            file.close()
        return errors
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return []

def main():
    file_to_check = 'blobmount/banksimlogs/20240614/tba/ADDL87_trades_20240614_1_20240812_010048.log'

    if check_file_exists(file_to_check):
        print(f"{file_to_check} exists.")
    else:
        print(f"{file_to_check} does not exist.")

    errors = check_for_errors(file_to_check)
    for error in errors:
        print(error)

if __name__ == "__main__":
    main()