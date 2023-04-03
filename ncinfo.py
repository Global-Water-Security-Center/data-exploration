"""See `python scriptname.py --help"""
import argparse
import rioxarray
import xarray


def main():
    parser = argparse.ArgumentParser(
        description='Dump netcat info on a file to command line.')
    parser.add_argument('raster_path', help='path to netcat file')
    args = parser.parse_args()
    print(xarray.open_dataset(args.raster_path))
    print(rioxarray.open_rasterio(args.raster_path))


if __name__ == '__main__':
    main()
