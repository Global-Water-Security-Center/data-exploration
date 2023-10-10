"""See `python scriptname.py --help"""
from datetime import datetime
import argparse
import glob
import os
import re

PATTERN = r'(.*QL)\d{8}(-.*)'
NEW_PATTERN = r'\g<1>{NEW_DATE}\g<2>'


def boolean_type(value):
    if value.lower() in ['true', 'false']:
        return value.lower() == 'true'
    else:
        raise argparse.ArgumentTypeError("Expected 'true' or 'false'")


def eight_digit_type(value):
    if len(value) != 8 or not value.isdigit():
        raise argparse.ArgumentTypeError("Expected exactly 8 digits")
    return value


def main():
    current_date = datetime.now().strftime('%Y%m%d')
    parser = argparse.ArgumentParser(description=(
        f'Script to rename files with the pattern '
        f'{PATTERN} to {NEW_PATTERN}.'))
    parser.add_argument(
        'directories_to_search', nargs='+',
        help='Path/pattern to directories to search')
    parser.add_argument(
        '--new_date', type=eight_digit_type, default=current_date, help=(
            'Date pattern to replace the matching pattern with, default is '
            f'current date as {current_date}.'))
    parser.add_argument(
        '--rename', type=boolean_type, help=(
            'Pass with an argument of True to do the rename, '
            'otherwise it lists what the renames will be.'))
    args = parser.parse_args()

    paths_to_search = [
        path
        for pattern in args.directories_to_search
        for path in glob.glob(pattern)]

    rename_file_list = []
    conflicting_file_list = []

    for dir_path in paths_to_search:
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            match = re.match(PATTERN, file_path)
            if not match:
                continue
            renamed_path = re.sub(PATTERN, NEW_PATTERN.format(
                NEW_DATE=args.new_date), file_path)

            if os.path.exists(renamed_path) and renamed_path != file_path:
                conflicting_file_list.append((file_path, renamed_path))
            else:
                rename_file_list.append((file_path, renamed_path))

    if conflicting_file_list:
        raise ValueError(
            'The following files if renamed will conflict with an existing '
            'file:\n' + '\n'.join([
                f' {path_a} -> {path_b}'
                for path_a, path_b in conflicting_file_list]))

    print('renaming: ' if args.rename else
          'DRY RUN, pass --rename True to rename:')

    for base_path, renamed_path in rename_file_list:
        if base_path == renamed_path:
            print(
                f'skipping {base_path} -> {renamed_path} because it is the '
                'same file')
            continue
        print(f' * {base_path} -> {renamed_path}')
        if args.rename:
            os.rename(base_path, renamed_path)

    if not args.rename:
        print('^^^^^^^^ DRY RUN, pass --rename True to rename ^^^^^^^^')


if __name__ == '__main__':
    main()
