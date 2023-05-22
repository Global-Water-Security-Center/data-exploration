"""See `python scriptname.py --help"""
from pathlib import Path
import os
import argparse
import re
import glob

PATTERN = r'(.*QL)\d{8}(-.*)'
NEW_PATTERN = r'\g<1>{NEW_DATE}\g<2>'

def main():
    parser = argparse.ArgumentParser(description=(
        f'Script to rename files with the pattern '
        f'{PATTERN} to {NEW_PATTERN}.'))
    parser.add_argument(
        'directories_to_search', nargs='+',
        help='Path/pattern to directories to search')
    parser.add_argument(
        '--new_date', required=True, type=str,
        help='Date pattern to replace the matching pattern with.')
    parser.add_argument(
        '--rename', type=bool, help=(
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
            if not os.path.isfile(file_path):
                continue
            match = re.match(PATTERN, file_path)
            if not match:
                continue
            renamed_path = re.sub(PATTERN, NEW_PATTERN.format(
                NEW_DATE=args.new_date), file_path)

            if os.path.exists(renamed_path):
                conflicting_file_list.append((file_path, renamed_path))
            else:
                rename_file_list.append((file_path, renamed_path))

    if conflicting_file_list:
        raise ValueError(
            'The following files if renamed will conflict with an existing '
            'file:\n' + '\n'.join([
                (' SAME FILE >>' if path_a == path_b else ' *') +
                f' {path_a} -> {path_b}'
                for path_a, path_b in conflicting_file_list]))

    print('renaming: ' if args.rename else
          'DRY RUN, pass --rename True to rename:')

    for base_path, renamed_path in rename_file_list:
        print(f' * {base_path} -> {renamed_path}')
        if args.rename:
            os.rename(base_path, renamed_path)

    if not args.rename:
        print('^^^^^^^^ DRY RUN, pass --rename True to rename ^^^^^^^^')


if __name__ == '__main__':
    main()
