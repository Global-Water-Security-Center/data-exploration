"""See `python scriptname.py --help"""
import argparse
import rioxarray
import xarray


def main():
    parser = argparse.ArgumentParser(
        description='Dump netcdf info on a file to command line.')
    parser.add_argument('raster_path', help='path to netcdf file')
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
            for var_name, var in nc_file.variables.items():
                # Look for attributes related to projections, like "proj4", "grid_mapping", etc.
                if 'proj4' in var.attrs:
                    proj4_string = var.attrs['proj4']
                    print(f'Proj4 string: {proj4_string}')
                if 'grid_mapping' in var.attrs:
                    grid_mapping_var = var.attrs['grid_mapping']
                    print(f'Grid mapping variable: {grid_mapping_var}')
                    # Get the attributes of the grid mapping variable for more details
                    grid_mapping_attrs = nc_file[grid_mapping_var].attrs
                    print(f'Grid mapping attributes: {grid_mapping_attrs}')
            break
        except ValueError:
            if decode_times is False:
                raise
            decode_times = False


if __name__ == '__main__':
    main()
