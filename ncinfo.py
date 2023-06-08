"""See `python scriptname.py --help"""
import argparse
import rioxarray
import xarray


def main():
    parser = argparse.ArgumentParser(
        description='Dump netcat info on a file to command line.')
    parser.add_argument('raster_path', help='path to netcat file')
    args = parser.parse_args()
    decode_times = True
    while True:
        try:
            print(rioxarray.open_rasterio(
                args.raster_path, decode_times=decode_times))
            print('DETAILS OF ALL VARS: ')
            nc_file = xarray.open_dataset(
                args.raster_path, decode_times=decode_times)
            for var in nc_file.variables:
                print(f"{var}:")
                for attr_name, attr_value in nc_file[var].attrs.items():
                    print(f"    {attr_name}: {attr_value}")
            break
        except ValueError:
            if decode_times is False:
                raise
            decode_times = False


if __name__ == '__main__':
    main()
