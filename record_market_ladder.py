"""
This is a script that download an odds market ladder for a certain market in
Betfair.
"""

import argparse
import csv
from datetime import datetime, timedelta
import logging
import os
import queue
import tempfile
import time
from typing import Dict, Tuple
import zipfile

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


def parse_command_line_args() -> Tuple[str, str, int, bool, bool, int]:
    """Parse command line arguments.

    Returns:
        Command line arguments: Market ID and output directory.
    """
    parser = argparse.ArgumentParser(
        description='A script that reads the live Betfair market ladder for a '
                    'certain market ID and saves the odds in a CSV file. The '
                    'market ladder will have virtual bets by default. This can '
                    'be disabled with flag --no-virtual. With virtual bets '
                    'there can be only be ten back prices and ten lay prices. '
                    'If --no-virtual is used, the full market ladder will be '
                    'downloaded.'
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
    parser.add_argument(
        '--no-virtual-bets',
        dest='no_virtual_bets',
        action='store_true',
        help='Disable virtual bets. Without virtual bets, the whole ladder of '
             'prices available to back or lay will be downloaded.'
    )
    parser.add_argument(
        '--in-play',
        dest='allow_inplay',
        action='store_true',
        help='Allow streaming in-play. By default, the data stream stops when '
             'an event turns in-play. To allow the stream to run until the end '
             'of the event, use this flat.'
    )
    parser.add_argument(
        '--mins-before-start',
        dest='mins_before_start',
        type=int,
        default=None,
        help='Amount of minutes to start streaming before the start of the '
             'event.'
    )
    args = parser.parse_args()

    return (
        args.market_id,
        args.output_dir,
        args.conflate_ms,
        args.no_virtual_bets,
        args.allow_inplay,
        args.mins_before_start
    )


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
        trading: Instance of the betfairlightweight api client.
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
    event_type_formatted = event_type.replace(' ', '-').replace('/', '-')
    event_formatted = event.replace(' ', '-').replace('/', '-')
    competition_formatted = competition.replace(' ', '-').replace('/', '-')
    market_name_formatted = market_name.replace(' ', '-').replace('/', '-')
    market_start_time_formatted = datetime.strftime(
        market_start_time, '%Y-%m-%dT%H-%M-%S'
    )

    file_name = (
        event_type_formatted + '_' +
        event_formatted + '_' +
        competition_formatted + '_' +
        market_name_formatted + '_' +
        market_start_time_formatted
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

    market_id, output_dir, conflate_ms, no_virtual_bets, allow_inplay,\
        mins_before_start = parse_command_line_args()

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

    # Wait to stream until a certain amount of minutes before the start
    if mins_before_start is not None:
        logger.info(
            "Waiting until %s minutes before the start of the event. Press "
            "Ctrl+C to quit.",
            mins_before_start
        )
        now = datetime.utcnow()
        try:
            while market_start_time - now >= timedelta(minutes=mins_before_start):
                time.sleep(1)
                now = datetime.utcnow()
        except KeyboardInterrupt:
            logger.info("Exiting program (Keyboard interrupt)")
            trading.logout()
            exit(0)
        if trading.session_expired:
            logger.info("Session expired. Logging in to Betfair again.")
            trading.login()

    # Output file path
    output_file_name = get_output_file_name(
        event_type, event, competition, market_name, market_start_time
    )
    output_csv_file = os.path.join(output_dir, output_file_name + '.csv')
    output_zip_file = os.path.join(output_dir, output_file_name + '.zip')

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
    if no_virtual_bets:
        market_data_fields = ['EX_MARKET_DEF', 'EX_ALL_OFFERS']
    else:
        market_data_fields = ['EX_MARKET_DEF', 'EX_BEST_OFFERS_DISP']
    market_data_filter = streaming_market_data_filter(
        fields=market_data_fields,
    )

    logger.info("Subscribing to the market")
    stream.subscribe_to_markets(
        market_filter=market_filter,
        market_data_filter=market_data_filter,
        conflate_ms=conflate_ms
    )

    logger.info("Starting the stream")
    stream.start(async_=True)

    logger.info(f"Saving data in file {output_csv_file}")
    with open(output_csv_file, 'w') as f:
        f_csv = csv.writer(f)

        csv_header = [
            'selection',
            'time',
            'price',
            'size',
            'side',
            'selection_status',
            'market_status',
            'in_play'
        ]
        f_csv.writerow(csv_header)

        market_snapshot_no = 0

        while True:
            try:
                market_books = output_queue.get()
                market_book = market_books[0]

                market_status = market_book.status
                market_inplay = market_book.inplay
                publish_time = market_book.publish_time

                # Stop the stream if the conditions are met
                if allow_inplay:
                    if market_status == 'CLOSED':
                        break
                else:
                    if market_status == 'CLOSED' or market_inplay is True:
                        break

                rows = []
                for runner in market_book.runners:
                    selection_id = runner.selection_id
                    selection_status = runner.status

                    for back in runner.ex.available_to_back:
                        rows.append(
                            (
                                selections[selection_id],
                                publish_time,
                                back.price,
                                back.size,
                                'back',
                                selection_status,
                                market_status,
                                market_inplay
                            )
                        )

                    for lay in runner.ex.available_to_lay:
                        rows.append(
                            (
                                selections[selection_id],
                                publish_time,
                                lay.price,
                                lay.size,
                                'lay',
                                selection_status,
                                market_status,
                                market_inplay
                            )
                        )

                f_csv.writerows(rows)

                market_snapshot_no = market_snapshot_no + 1
                logger.info("Market snapshot #%s stored.", market_snapshot_no)

            except KeyboardInterrupt:
                logger.info("Exiting program (Keyboard interrupt)")
                break

    logger.info(
        "Stopping the stream and logging out from Betfair. This may take a few "
        "seconds."
    )
    stream.stop()
    trading.logout()

    logger.info("Compressing the CSV file into ZIP file %s", output_zip_file)
    with zipfile.ZipFile(output_zip_file, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        zip_f.write(output_csv_file, os.path.basename(output_csv_file))

    return output_csv_file


if __name__ == "__main__":

    data_collection_pipeline()
