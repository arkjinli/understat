import aiohttp
import asyncio
import logging
import itertools

import random
import json
import re
import os

from bs4 import BeautifulSoup


ROOTDIR = 'data'
BASEURL = 'https://understat.com/'
PATTERN = re.compile(r"([a-zA-Z]+)\s+=\s+JSON.parse\(\'(.*?)\'\)")

class ThrottlingError(Exception):
    pass

async def tag_and_save(key, coroutine):
    '''
    attach a tag to coroutine and save to hard disks with path dependent on tags
    '''
    data = await coroutine
    path = os.path.join(ROOTDIR, *key)
    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(path, 'w') as f:
        f.write(data)
    return key, data

async def get_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            if html.strip() == 'closed.php':
                raise ThrottlingError()
    return html

async def get_data(url):
    while True:
        try:
            html = await get_page(url)
        except ThrottlingError:
            cool_down = random.randint(1, 5) + random.random()
            logging.warning('%s throttling, cool down for %.2f secs', url, cool_down)
            await asyncio.sleep(cool_down)
            continue
        else:
            break

    soup = BeautifulSoup(html, "html.parser")

    data = dict()
    for script in soup.find_all('script'):
        result = PATTERN.search(script.text)
        if result:
            key, val = result.groups()
            val_byte = bytes(val, 'utf-8')
            val_json = json.loads(val_byte.decode('unicode_escape'))
            data[key] = val_json
    # dict data is not hashable
    return json.dumps(data)

async def process_league_pages(leagues, seasons):
    tasks = []
    for league, season in itertools.product(leagues, seasons):
        season_start, _ = season.split('-')
        url = os.path.join(BASEURL, 'league', league, season_start)
        tasks.append(
            tag_and_save(('league', league, season), get_data(url))
        )
    return await asyncio.gather(*tasks)

async def process_team_pages(team_seasons):
    tasks = []
    for team_name, season in team_seasons:
        season_start, _ = season.split('-')
        url = os.path.join(BASEURL, 'team', team_name, season_start)
        tasks.append(
            tag_and_save(('team', team_name, season), get_data(url))
        )
    await asyncio.gather(*tasks)

async def process_player_pages(player_ids):
    tasks = []
    for player_id in player_ids:
        url = os.path.join(BASEURL, 'player', player_id)
        tasks.append(
            tag_and_save(('player', player_id), get_data(url))
        )
    await asyncio.gather(*tasks)

async def process_match_pages(match_ids):
    tasks = []
    for match_id in match_ids:
        url = os.path.join(BASEURL, 'match', match_id)
        tasks.append(
            tag_and_save(('match', match_id), get_data(url))
        )
    await asyncio.gather(*tasks)

async def loop_run_in_batch(func, arg_list, batch_size):
    numbatches = (len(arg_list) + batch_size - 1) // batch_size
    logging.info('split %-20s in %3d batches (len(arg_list)=%5d, batch_size=%2d)', func.__name__, numbatches, len(arg_list), batch_size)
    for i in range(numbatches):
        start, stop = i*batch_size, min((i+1)*batch_size, len(arg_list))
        await func(arg_list[start:stop])
        logging.info('%-20s batch %2d: got %s', func.__name__, i, stop)

async def main(leagues, seasons, batch_size=10):
    logging.info('process `league/season` pages...')
    group_leagues = await process_league_pages(leagues, seasons)
    data = {
        key: json.loads(val) for key, val in group_leagues
    }

    logging.info('get team_seasons and player_ids...')
    team_seasons = set()
    for (_, _, season), val in data.items():
        team_seasons.update(
            {(team['title'], season) for team_id, team in val['teamsData'].items()}
        )
    player_ids = set()
    for val in data.values():
        player_ids.update(
            {player['id'] for player in val['playersData']}
        )

    match_ids = set()
    for val in data.values():
        match_ids.update(
            {match['id'] for match in val['datesData']}
        )

    logging.info('process `team/season` & `player` pages...')
    await asyncio.gather(
        loop_run_in_batch(process_team_pages, list(team_seasons), batch_size),
        loop_run_in_batch(process_player_pages, list(player_ids), batch_size)
    )

    logging.info('process `match` pages...')
    await loop_run_in_batch(process_match_pages, list(match_ids), batch_size*2)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    seasons = ['2014-2015', '2015-2016', '2016-2017', '2017-2018', '2018-2019']
    # leagues = list(filter(lambda name: name != 'stats', os.listdir(os.path.join(ROOTDIR, 'league'))))
    leagues = ['EPL', 'La_liga', 'Bundesliga', 'Serie_A', 'Ligue_1', 'RFPL']

    logging.info('seasons = %s', seasons)
    logging.info('leagues = %s', leagues)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(leagues, seasons))
