"""
This is a script that download an odds market ladder for a certain market in
Betfair.
"""

import argparse
import csv
from datetime import datetime
import logging
import os
import queue
import tempfile
from typing import Dict, Tuple

import betfairlightweight as bfl
from betfairlightweight.filters import (
    streaming_market_filter,
    streaming_market_data_filter,
)


logger = logging.getLogger(__name__)


# Get the environment variables
username = os.environ['BETFAIR_USERNAME']
password = os.environ['BETFAIR_PASSWORD']
app_key = os.environ['BETFAIR_APP_KEY']
cert_file = os.environ['BETFAIR_CERT_FILE']
cert_key_file = os.environ['BETFAIR_CERT_KEY_FILE']


def parse_command_line_args() -> Tuple[str, str, int]:
    """Parse command line arguments.

    Returns:
        Command line arguments: Market ID and output directory.
    """
    parser = argparse.ArgumentParser(
        description='A script that reads the live Betfair market ladder for a '
                    'certain market ID and saves the odds in a CSV file.'
    )
    parser.add_argument('market_id', help='Betfair market ID.')
    parser.add_argument(
        '-o', '--output-dir',
        dest='output_dir',
        default=tempfile.gettempdir(),
        help='Path of the directory where to save the output CSV file. '
             'The default path is to the system temporary directory.'
    )
    parser.add_argument(
        '-c', '--conflate-ms',
        dest='conflate_ms',
        type=int,
        default=50,
        help='Conflation rate in milliseconds (bounds are 0 to 120000).'
             'The default value is 50 milliseconds.'
    )
    args = parser.parse_args()

    return args.market_id, args.output_dir, args.conflate_ms


def get_event_info(
        trading: bfl.apiclient.APIClient,
        market_id: str
) -> Tuple[str, str, str]:
    """Get the event information for a Betfair market ID.

    Args:
        trading: Instance of ``betfairlightweight.apiclient.APIClient``.
        market_id: ID of the Betfair market.

    Returns:
        Event type, event name and competition name.
    """
    market_filter = bfl.filters.market_filter(market_ids=[market_id])

    event_type = (
        trading
        .betting
        .list_event_types(filter=market_filter)[0]
        .event_type
        .name
    )

    event = (
        trading
        .betting
        .list_events(filter=market_filter)[0]
        .event
        .name
    )

    competition = (
        trading
        .betting
        .list_competitions(filter=market_filter)[0]
        .competition
        .name
    )

    return event_type, event, competition


def get_market_info(
        trading: bfl.apiclient.APIClient,
        market_id: str
) -> Tuple[str, datetime, Dict[str, str]]:
    """Get the market information from a Betfair market ID.

    Args:
        trading: Instance of ``betfairlightweight.apiclient.APIClient``.
        market_id: ID of the Betfair market.

    Returns:
        Market name, market start datetime (that is, when the event starts),
        market selections mapping from ID to name.
    """
    market_filter = bfl.filters.market_filter(market_ids=[market_id])

    market = (
        trading
        .betting
        .list_market_catalogue(
            filter=market_filter,
            market_projection=['MARKET_START_TIME', 'RUNNER_DESCRIPTION']
        )[0]
    )

    market_name = market.market_name
    market_start_time = market.market_start_time

    selections = {}
    for runner in market.runners:
        selections[runner.selection_id] = runner.runner_name

    return market_name, market_start_time, selections


def get_output_file_name(
        event_type: str,
        event: str,
        competition: str,
        market_name: str,
        market_start_time: datetime
) -> str:
    """Build the name of the output CSV file.

    Args:
        event_type: Name of the event type.
        event: Name of the event.
        competition: Name of the competition.
        market_name: Name of the market.
        market_start_time: Start time of the market (or event).

    Returns:
        Name of the output CSV file.
    """
    event_type_formatted = event_type.replace(' ', '-')
    event_formatted = event.replace(' ', '-')
    competition_formatted = competition.replace(' ', '-')
    market_name_formatted = market_name.replace(' ', '-')
    market_start_time_formatted = datetime.strftime(
        market_start_time, '%Y-%m-%dT%H-%M-%S'
    )

    file_name = (
        event_type_formatted + '_' +
        event_formatted + '_' +
        competition_formatted + '_' +
        market_name_formatted + '_' +
        market_start_time_formatted +
        '.csv'
    )

    return file_name


def data_collection_pipeline() -> str:
    """Pipeline to collect Betfair odds market ladder streaming data.

    Returns:
        Path of the output CSV file.
    """
    logging.basicConfig(
        format='%(levelname)-8s | %(asctime)s | %(name)s:  %(message)s',
        level=logging.INFO,
        # stream=sys.stdout
    )

    market_id, output_dir, conflate_ms = parse_command_line_args()

    trading = bfl.APIClient(
        username=username,
        password=password,
        app_key=app_key,
        cert_files=[cert_file, cert_key_file]
    )

    logger.info("Logging in to Betfair")
    trading.login()

    # Event and market information
    event_type, event, competition = get_event_info(trading, market_id)
    market_name, market_start_time, selections = get_market_info(
        trading, market_id
    )

    # Output file path
    output_file_name = get_output_file_name(
        event_type, event, competition, market_name, market_start_time
    )
    output_file = os.path.join(output_dir, output_file_name)

    # Market stream
    logger.info("Initialising output queue")
    output_queue = queue.Queue()

    logger.info("Initialising Betfair stream listener")
    listener = bfl.StreamListener(output_queue)

    logger.info("Creating the Betfair market stream")
    stream = trading.streaming.create_stream(listener=listener)

    logger.info("Setting the market filter to market_id=%s", market_id)
    market_filter = streaming_market_filter(market_ids=[market_id])

    logger.info("Initialising streaming market data filter")
    market_data_filter = streaming_market_data_filter(
        fields=['EX_MARKET_DEF', 'EX_ALL_OFFERS', 'EX_TRADED'],
    )

    logger.info("Subscribing to the market")
    stream.subscribe_to_markets(
        market_filter=market_filter,
        market_data_filter=market_data_filter,
        conflate_ms=conflate_ms
    )

    logger.info("Starting the stream")
    stream.start(async_=True)

    logger.info(f"Saving data in file {output_file}")
    with open(output_file, 'w') as f:
        f_csv = csv.writer(f)

        csv_header = [
            'selection',
            'time',
            'price',
            'size',
            'side'
        ]
        f_csv.writerow(csv_header)

        market_snapshot_no = 0

        while True:
            try:
                market_books = output_queue.get()
                market_book = market_books[0]

                publish_time = market_book.publish_time

                rows = []
                for runner in market_book.runners:
                    selection_id = runner.selection_id

                    for back in runner.ex.available_to_back:
                        rows.append(
                            (
                                selections[selection_id],
                                publish_time,
                                back.price,
                                back.size,
                                'back'
                            )
                        )

                    for lay in runner.ex.available_to_lay:
                        rows.append(
                            (
                                selections[selection_id],
                                publish_time,
                                lay.price,
                                lay.size,
                                'lay'
                            )
                        )

                f_csv.writerows(rows)

                market_snapshot_no = market_snapshot_no + 1
                logger.info("Market snapshot #%s stored.", market_snapshot_no)

            except KeyboardInterrupt:
                logger.info("Exiting program (Keyboard interrupt)")
                break

    logger.info("Stopping the stream and logging out from Betfair.")
    stream.stop()
    trading.logout()

    return output_file


if __name__ == "__main__":

    data_collection_pipeline()
