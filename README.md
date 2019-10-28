# Betfair Data

Simple script to download live market ladder data of a single market from Betfair and store it into an SQLite database.

## Install

Install the Python requirement packages with the following command.

```shell
pip install -r requirements.txt
```

## Run

To run the script, first, export the Betfair credentials details as environment variables.

```shell
export BETFAIR_USERNAME=<betfair_username>
export BETFAIR_PASSWORD=<betfair_password>
export BETFAIR_APP_KEY=<betfair_app_key>
export BETFAIR_CERT_FILE=<path_to_the_betfair_certificate_file>
export BETFAIR_CERT_KEY_FILE=<path_to_the_betfair_certificate_key_file>
```

In the above code, substitute the placeholders, like `<betfair_username>` with your details.

Then, invoke the script using Python.

```shell
python record_market_ladder.py <market_id>
```

Here, `<market_id>` is the ID of the market whose live market ladder data you wish to download.

To terminate the program, press Ctrl + C.

For more information on how to use the script, you may use the help flag.

```shell
python record_market_ladder.py --help
```

It is also possible to run several recording tasks in the background by using the following command.

```shell
cat market_ids.txt | xargs -I market_id screen -d -m bash -c "python record_market_ladder.py market_id"
```

Here, `market_ids.txt` is a file containing a list of market IDs, like in the following example.

```
1.160287874
1.160287758
1.160287990
1.160288106
```

There will be one recording task for each market ID in the list.

