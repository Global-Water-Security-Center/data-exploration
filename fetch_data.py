import argparse
import collections
import configparser

GLOBAL_INI_PATH = 'defaults.ini'

def main():
	global_config = configparser.ConfigParser(allow_no_value=True)
    global_config.read(GLOBAL_INI_PATH)
    global_config.read(scenario_config_path)

if __name__ == '__main__':
	main()
