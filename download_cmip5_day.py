import argparse
import ee
import logging
import sys
import os
import requests

logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


def download_image(image, target_path):
    if not os.path.exists(target_path):
        url = image.getDownloadUrl({
            'scale': 27830,
            'format': 'GEO_TIFF'
        })
        LOGGER.debug(f'saving {target_path}')
        response = requests.get(url)
        with open(target_path, 'wb') as fd:
            fd.write(response.content)
    else:
        LOGGER.info(f'{target_path} already exists, not overwriting')


def main():
    _ = argparse.ArgumentParser(description=(
        'Not a command line script. Used to download and inspect the size of '
        'a GEE CMIP5 daily raster to estimate total offline storage.'))
    ee.Initialize()
    dataset = ee.ImageCollection('NASA/NEX-GDDP').filter(ee.Filter.date('2018-07-01', '2018-07-02')).first();
    download_image(dataset, 'cmip5.tif')


if __name__ == '__main__':
    main()
