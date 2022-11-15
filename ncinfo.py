"""Demo of how to slice the GDM dataset by date range and save to geotiff"""
import argparse
import rioxarray
import xarray


def main():
    parser = argparse.ArgumentParser(description='NETCat info files')
    parser.add_argument('raster_path', help='path to netcat file')
    args = parser.parse_args()
    print(xarray.open_dataset(args.raster_path))
    print(rioxarray.open_rasterio(args.raster_path))


if __name__ == '__main__':
    main()
