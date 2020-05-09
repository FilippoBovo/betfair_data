"""
This is a script that converts a zipped SQL file with the market data, obtained
from script 'record_market_ladder.py', to a Parquet file with a much smaller
size and faster loading times.
"""

import argparse
import logging
from pathlib import Path
import sqlite3
import tempfile
from typing import Union, Tuple
from zipfile import ZipFile

import pandas as pd


logger = logging.getLogger(__name__)


def _load_market_data(
        zip_file: Path
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame
]:
    """Load the market data from a ZIP file containing an SQLite database.

    Args:
        zip_file: Path of the ZIP file containing the SQLite database.

    Returns:
        Pandas DataFrames for the market status, selection status,
            available to back, available to lay and traded volume data.
    """
    logger.info("Loading data from file: %s", zip_file)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract SQLite database from ZIP file to temporary directory
        with ZipFile(zip_file) as zip_f:
            file_member = zip_f.filelist[0]
            sqlite_file = zip_f.extract(file_member, path=temp_dir)

        connection = sqlite3.connect(sqlite_file)

        # Load market status
        market_status = pd.read_sql_query(
            sql="select * from market_status;", con=connection
        )
        market_status["date_time"] = pd.to_datetime(
            market_status["date_time"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        market_status = market_status.astype(
            {
                'status': 'category',
                'inplay': 'category'
            }
        )

        # Load selection status
        selection_status = pd.read_sql_query(
            sql="select * from selection_status;", con=connection
        )
        selection_status["date_time"] = pd.to_datetime(
            selection_status["date_time"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        selection_status = selection_status.astype(
            {
                'selection': 'category',
                'status': 'category'
            }
        )

        # Load available to back
        available_to_back = pd.read_sql_query(
            sql="select * from available_to_back;", con=connection
        )
        available_to_back["date_time"] = pd.to_datetime(
            available_to_back["date_time"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        available_to_back = available_to_back.astype({'selection': 'category'})

        # Load available to lay
        available_to_lay = pd.read_sql_query(
            sql="select * from available_to_lay;", con=connection
        )
        available_to_lay["date_time"] = pd.to_datetime(
            available_to_lay["date_time"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        available_to_lay = available_to_lay.astype({'selection': 'category'})

        # Load traded volume
        traded_volume = pd.read_sql_query(
            sql="select * from traded_volume;", con=connection
        )
        traded_volume["date_time"] = pd.to_datetime(
            traded_volume["date_time"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        traded_volume = traded_volume.astype({'selection': 'category'})

    return (
        market_status,
        selection_status,
        available_to_back,
        available_to_lay,
        traded_volume
    )


class MarketData:
    def __init__(self, zip_file: Path):
        """Initialise the MarketData class.

        Args:
            zip_file: Path of the zip file containing the SQLite database with
                the market data.
        """
        if zip_file.suffix != '.zip':
            raise ValueError("The file is not a zip file.")

        self.zip_file = zip_file
        self.data_is_loaded = False

        self._market_status: Union[pd.DataFrame, None] = None
        self._selection_status: Union[pd.DataFrame, None] = None
        self._available_to_back: Union[pd.DataFrame, None] = None
        self._available_to_lay: Union[pd.DataFrame, None] = None
        self._traded_volume: Union[pd.DataFrame, None] = None

    def load_data(self):
        """Load the market data."""
        (
            self._market_status,
            self._selection_status,
            self._available_to_back,
            self._available_to_lay,
            self._traded_volume
        ) = _load_market_data(self.zip_file)

        self.data_is_loaded = True

    @property
    def market_status(self) -> pd.DataFrame:
        """Market status data.

        Returns:
            Pandas DataFrame with the market status data.
        """
        if not self.data_is_loaded:
            self.load_data()

        return self._market_status

    @property
    def selection_status(self) -> pd.DataFrame:
        """Selection status data.

        Returns:
            Pandas DataFrame with the selection status data.
        """
        if not self.data_is_loaded:
            self.load_data()

        return self._selection_status

    @property
    def available_to_back(self) -> pd.DataFrame:
        """Available to back data.

        Returns:
            Pandas DataFrame with the available to back data.
        """
        if not self.data_is_loaded:
            self.load_data()

        return self._available_to_back

    @property
    def available_to_lay(self) -> pd.DataFrame:
        """Available to lay data.

        Returns:
            Pandas DataFrame with the available to lay data.
        """
        if not self.data_is_loaded:
            self.load_data()

        return self._available_to_lay

    @property
    def traded_volume(self) -> pd.DataFrame:
        """Traded volume data.

        Returns:
            Pandas DataFrame with the traded volume data.
        """
        if not self.data_is_loaded:
            self.load_data()

        return self._traded_volume


def merge_market_data(market_data: MarketData) -> pd.DataFrame:
    """Merge the different Pandas DataFrames in MarketData into a single one.

    Args:
        market_data: Instance of the MarketData class.

    Returns:
        Merged Pandas DataFrame with the following columns - date_time,
            selection, price, back_size, lay_size, traded_size,
            selection_status, market_status, inplay.
    """
    logging.info("Merging market data into a single Pandas DataFrame.")

    # Rename columns
    available_to_back = (
        market_data.available_to_back
        .rename(columns={'size': 'back_size'})
    )
    available_to_lay = (
        market_data.available_to_lay
        .rename(columns={'size': 'lay_size'})
    )
    traded_volume = (
        market_data.traded_volume
        .rename(columns={'size': 'traded_size'})
    )
    selection_status = (
        market_data.selection_status
        .rename(columns={'status': 'selection_status'})
    )
    market_status = (
        market_data.market_status
        .rename(columns={'status': 'market_status'})
    )

    # Merge the different DataFrames
    merged_data = pd.merge(
        left=available_to_back,
        right=available_to_lay,
        on=['date_time', 'selection', 'price'],
        how='outer'
    )
    merged_data = pd.merge(
        left=merged_data,
        right=traded_volume,
        on=['date_time', 'selection', 'price'],
        how='outer'
    )
    merged_data = pd.merge(
        left=merged_data,
        right=selection_status,
        on=['date_time', 'selection'],
        how='left'
    )
    merged_data = pd.merge(
        left=merged_data,
        right=market_status,
        on=['date_time'],
        how='left'
    )

    # Sort, reset index and set types
    merged_data = (
        merged_data
        .sort_values(by=['date_time', 'selection', 'price'])
        .reset_index(drop=True)
        .astype(
            {
                'selection': 'category',
                'selection_status': 'category',
                'market_status': 'category',
                'inplay': 'category'
            }
        )

    )

    return merged_data


def parse_command_line_args() -> Tuple[Path, Path]:
    """Parse command line arguments.

    Returns:
        Command line arguments: Path of the zipped SQL file with the market data
            and path of the output directory.
    """
    parser = argparse.ArgumentParser(
        description='A script that converts a zipped SQL file with the market '
                    'data into a Parquet file with a much smaller size and '
                    'faster loading times.'
    )
    parser.add_argument(
        'zipped_sql_file',
        type=Path,
        help='Zipped SQL file with the market data.'
    )
    parser.add_argument(
        '-o', '--output-dir',
        dest='output_dir',
        type=Path,
        default=Path(tempfile.gettempdir()) / 'market_data_parquet',
        help='Path of the directory where to save the output Parquet file. '
             'The default path is directory "market_data_parquet" in the '
             'system temporary directory.'
    )
    args = parser.parse_args()

    return args.zipped_sql_file, args.output_dir


def convert_sql_to_parquet():
    """Convert the zipped SQL file with the market data to a Parquet file."""
    logging.basicConfig(
        format='%(levelname)-8s | %(asctime)s | %(name)s:  %(message)s',
        level=logging.INFO
    )

    zipped_sql_file, output_dir = parse_command_line_args()

    if not output_dir.exists():
        output_dir.mkdir()

    parquet_file = output_dir / (zipped_sql_file.stem + '.parquet')

    market_data = MarketData(zipped_sql_file)
    market_data.load_data()

    data = merge_market_data(market_data)
    del market_data

    logger.info("Saving the data into Parquet file %s", parquet_file.absolute())
    data.to_parquet(parquet_file.as_posix())


if __name__ == "__main__":

    convert_sql_to_parquet()
