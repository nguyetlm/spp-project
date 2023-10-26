import datetime
import glob
import os
from tqdm import tqdm
import h5py
import numpy as np
import requests
from dateutil.relativedelta import relativedelta

from src.logging import get_logger

logger = get_logger("test_downloader", log_level="DEBUG", to_file=True)

def imerg_url(base_url: str, product: str, version: int, year: int):
    """
    url format: '3IMERGHH': '{mission}_{product}.{version:02}/{year}/{dayofyear:03}/3B-HHR.MS.MRG.3IMERG.{date}-S{time_start}-E{time_end}.{minutes}.V{version:02}B.HDF5'
    """
    year_start = datetime.datetime(year,1,1)
    year_end = datetime.datetime(year,12,31)

    n_days = (year_end - year_start).days + 1
    # n_urls = (n_days) * 48

    for i in range(n_days):
        date_to_dl = (year_start + datetime.timedelta(days=i)).replace(hour=0, minute=0, second=0)
        date_to_dl_str = date_to_dl.strftime("%Y%m%d")
        # day = date_to_dl.day
        # month = date_to_dl.month
        i_str = str(i+1).zfill(3)
        for j in range(48):
            time_delta = j*30
            time_delta_str = str(time_delta).zfill(4)
            dt_start = date_to_dl + relativedelta(minutes=time_delta)
            dt_start_str = dt_start.strftime("%H%M%S")
            dt_end = (date_to_dl + relativedelta(minutes=time_delta + 30)) - relativedelta(seconds=1)
            dt_end_str = dt_end.strftime("%H%M%S")

            # print(dt_start, dt_end)
            url_prefix = f"{base_url}/{product}.{str(version).zfill(2)}/{year}/{i_str}/"
            file_name = f"3B-HHR.MS.MRG.3IMERG.{date_to_dl_str}-S{dt_start_str}-E{dt_end_str}.{time_delta_str}.V{str(version).zfill(2)}B.HDF5"

            yield url_prefix, file_name

if __name__ == "__main__":
    base_url = "https://gpm1.gesdisc.eosdis.nasa.gov/data"
    product = "GPM_L3/GPM_3IMERGHH"
    version = 6
    year = 2013
    num_days = 2
    data_path = r"E:\99 ongoing\spp-project\data"

    # n_iters = num_days * 48
    n_iters = 1
    with tqdm(total=n_iters) as pbar:
        for i, (url_prefix, file_name) in enumerate(imerg_url(base_url, product, version, year)):
            if i > n_iters - 1:
                break

            url = url_prefix + file_name
            url_data = requests.get(url, allow_redirects=True)
            status = url_data.status_code
            # print(f"url: {url}, status: {status}, i: {i}")
            if status != 404:
                if not os.path.exists(os.path.join(data_path,file_name)):
                    with open(os.path.join(data_path,file_name), "wb") as f:
                        f.write(url_data.content)
                else:
                    logger.info(f"File already downloaded: {file_name}")
            else:
                logger.info(f"File does not exist in the server: {file_name}")
            pbar.update(1)
            