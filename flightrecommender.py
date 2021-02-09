#!/usr/bin/env python3
"""
Flight-recommender

Main executable script.

Concept:
- Download recent flights from opensky
- Filter:
    - Operator
    - Departure/arrival
    - Tailnumber
    - ICAO region
- Rank:
    - Flight time
    - Departure/arrival
    - Tailnumber
    - Weather
"""


import json
import requests
import datetime
import time
import re
from diskcache import Cache

import logging


cache = Cache('cache')


def request_json(url, params={}):
    r = requests.get(url, params=params)
    logging.debug(f'Request url: {r.url}')
    while r.status_code == 503:
        logging.debug('Temporarily unavailable (503), retrying...')
        time.sleep(1.0)
        r = requests.get(url, params=params)
    if r.status_code == 404:
        return []
    elif r.status_code is not 200:
        raise RuntimeError(f'Request status not OK (200), namely: {r.status_code}.')
    return json.loads(r.text)


def opensky_get_flights_segment(begin_unix: int, end_unix: int):
    logging.debug(f'Get flights between {begin_unix} and {end_unix}...')
    cachable = (begin_unix % 3600) == 0 and (end_unix % 3600) == 0
    # Read from cache if available
    if cachable and begin_unix in cache:
        logging.debug('Available in cache!')
        return cache[begin_unix]
    # Read from opensky
    logging.debug('Read from opensky-network.org...')
    f = request_json('https://opensky-network.org/api/flights/all', {
        'begin': begin_unix,
        'end': end_unix,
    })
    # Cache results if possible
    if cachable and len(f) is not 0:
        logging.debug('Write to cache for future reference...')
        cache[begin_unix] = f
    # Return
    return f


def opensky_get_flights(begin: datetime.datetime, end: datetime.datetime):
    # Split time into 1hr segments
    begin_unix, end_unix = int(begin.timestamp()), int(end.timestamp())
    steps = [begin_unix]
    while (end_unix - steps[-1]) > 3600:
        next_step = steps[-1] + 3600
        next_step -= next_step % 3600  # Round to whole hours for caching
        steps.append(next_step)
    steps.append(end_unix)
    # Download flight data
    flights = []
    for os_begin_unix, os_end_unix in zip(steps, steps[1:]):
        flights += opensky_get_flights_segment(os_begin_unix, os_end_unix)
    return flights


def filter_by_region(flights, icao_regions):
    logging.debug(f'Filtering by ICAO regions {icao_regions}...')
    filtered = []
    for f in flights:
        dep_ok, arr_ok = False, False
        for region in icao_regions:
            if f['estDepartureAirport'] is not None and f['estDepartureAirport'].startswith(region):
                dep_ok = True
            if f['estArrivalAirport'] is not None and f['estArrivalAirport'].startswith(region):
                arr_ok = True
        if dep_ok and arr_ok:
            filtered.append(f)
    logging.debug(f'Done. {len(filtered)} flights remain.')
    return filtered


def filter_by_operator(flights, operators):
    logging.debug(f'Filtering by operators {operators}...')
    filtered = []
    for f in flights:
        op_ok = False
        if f['callsign'] is not None:
            for op in operators:
                if f['callsign'].startswith(op):
                    op_ok = True
            if op_ok:
                filtered.append(f)
    logging.debug(f'Done. {len(filtered)} flights remain.')
    return filtered


@cache.memoize()
def opensky_get_aircraft(icao24):
    logging.debug(f'Request aircraft information for {icao24}...')
    return request_json(f'https://opensky-network.org/api/metadata/aircraft/icao/{icao24}')


def get_aircraft_from_flights(flights):
    logging.debug(f'Loading aircraft data...')
    ac = {}
    for f in flights:
        if f['icao24'] is not None:
            ac[f['icao24']] = opensky_get_aircraft(f['icao24'])
    return ac


def filter_by_aircraft_type(flights, aircraft, allowed_types):
    logging.debug(f'Filtering by aircraft type {allowed_types}...')
    filtered = []
    for f in flights:
        if f['icao24'] in aircraft:
            ac = aircraft[f['icao24']]
            if 'typecode' in ac and ac['typecode'] in allowed_types:
                filtered.append(f)
    logging.debug(f'Done. {len(filtered)} flights remain.')
    return filtered


def score_by_flight_time(flights, min_time, max_time, penalty_per_min):
    logging.debug(f'Scoring by flight time between {min_time} and {max_time} minutes...')
    min_time_dt = datetime.timedelta(minutes=min_time)
    max_time_dt = datetime.timedelta(minutes=max_time)
    for f in flights:
        time_dep = datetime.datetime.fromtimestamp(int(f['firstSeen']), tz=datetime.timezone.utc)
        time_arr = datetime.datetime.fromtimestamp(int(f['lastSeen']), tz=datetime.timezone.utc)
        flight_time = time_arr - time_dep
        if flight_time < min_time_dt:
            f['score'] -= penalty_per_min * (min_time_dt - flight_time).total_seconds() / 60
        if flight_time > max_time_dt:
            f['score'] -= penalty_per_min * (flight_time - max_time_dt).total_seconds() / 60


def score_by_registration(flights, aircraft, registrations, score_match):
    logging.debug(f'Scoring registrations {registrations}...')
    for f in flights:
        if f['icao24'] not in aircraft:
            continue
        ac = aircraft[f['icao24']]
        reg = re.sub(r'[^A-Z0-9]', '', ac['registration'].upper())
        if reg in registrations:
            f['score'] += score_match


def score_by_airport(flights, airports):
    logging.debug(f'Scoring airports {[k for k in airports.keys()]}...')
    for f in flights:
        for ap, score in airports.items():
            if f['estDepartureAirport'].startswith(ap):
                f['score'] += score
            if f['estArrivalAirport'].startswith(ap):
                f['score'] += score


def flightrecommender(*args, **kwargs):
    conf = {}
    if 'config_json' in kwargs:
        with open(kwargs['config_json'], 'rt') as f:
            conf = json.load(f)

    # Load flights within search interval
    time_now = datetime.datetime.now(datetime.timezone.utc)
    time_start = time_now - datetime.timedelta(hours=conf['search']['time_interval_h'])
    flights = opensky_get_flights(time_start, time_now)

    # Filters
    if 'icao_region' in conf['filter']:
        flights = filter_by_region(flights, conf['filter']['icao_region'])
    if 'operator' in conf['filter']:
        flights = filter_by_operator(flights, conf['filter']['operator'])

    # Get aircraft data for remaining flights
    aircraft = get_aircraft_from_flights(flights)

    # Filters pt 2
    if 'aircraft_type' in conf['filter']:
        flights = filter_by_aircraft_type(flights, aircraft, conf['filter']['aircraft_type'])

    # Ranking
    for f in flights:
        f['score'] = 0.0
    if 'flight_time' in conf['rank']:
        score_by_flight_time(flights,
                             conf['rank']['flight_time']['min'],
                             conf['rank']['flight_time']['max'],
                             conf['rank']['flight_time']['penalty_per_min'])
    if 'registration' in conf['rank']:
        score_by_registration(flights, aircraft,
                              conf['rank']['registration']['value'],
                              conf['rank']['registration']['score_match'])
    if 'airport' in conf['rank']:
        score_by_airport(flights, conf['rank']['airport'])

    # Show results
    for f in sorted(flights, key=lambda fl: (-fl['score'], fl['callsign'])):
        dep_time = datetime.datetime.fromtimestamp(f['firstSeen'], tz=datetime.timezone.utc)
        dep_time_str = '{:02d}{:02d}Z'.format(dep_time.hour, dep_time.minute)
        registration = '?'
        typecode = '?'
        if f['icao24'] in aircraft:
            ac = aircraft[f['icao24']]
            registration = ac['registration']
            typecode = ac['typecode']
        print(f"{int(f['score']):>4}:\t{f['estDepartureAirport']} - {f['estArrivalAirport']}\t{dep_time_str}\t{f['callsign']}\t{registration} ({typecode})")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Flight recommender')
    parser.add_argument('config_json', type=str)
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    flightrecommender(**vars(args))
