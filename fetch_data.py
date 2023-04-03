import argparse
import collections
import configparser

GLOBAL_INI_PATH = 'defaults.ini'


def main():
    """Entry point."""
    global_config = configparser.ConfigParser(allow_no_value=True)
    global_config.read(GLOBAL_INI_PATH)
    global_config = global_config['defaults']
    available_commands = global_config['available_commands'].split(',')
    available_data = global_config['available_data'].split(',')
    parser = argparse.ArgumentParser(description='Data platform entry point')
    parser.add_argument('command', help=(
        'Command to execute, one of: ' + ', '.join(available_commands)))
    parser.add_argument('dataset_id', help=(
        'Dataset ID to operate on, one of: ' + ', '.join(available_data)))
    args = parser.parse_args()


if __name__ == '__main__':
    main()
