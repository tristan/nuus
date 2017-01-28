import click
import re

from sqlalchemy.sql import select
from sqlalchemy.sql.expression import and_

from nuus.database import engine, tables
from nuus.nzb import create_nzb, is_release_complete

from nuus.sabnzbd import SabnzbdClient

from nuus import settings

re_anime = re.compile(
    '\[(?P<group>[^\]]+)\][\s\._]?(?P<name>.+?)[\s\._]-[\s\._](?P<episode>\d{2,3})[\s\._](?:v(?P<version>\d+))?(?:[\s\._]?[\[\(](?P<quality>.+?)[\]\)])?'
    #'\[(?P<group>[^\]]+)\][\s\._]?(?P<name>.+?)[\s\._-]+(?P<episode>\d{2,3})(?:v(?P<version>\d+))?(?:[\s\._]?[\[\(](?P<quality>.+?)[\]\)])?'
)

class Application(object):
    def add_show(self, group, name, quality=None, start=1):
        conn = engine.connect()
        try:
            show = conn.execute(select([tables.shows]).where((tables.shows.c.name == name) & (tables.shows.c.group == group))).fetchone()
            if show and quality == show['quality']:
                raise Exception("[%s] %s already added." % (group, name))
            elif show:
                show['quality'] = quality
            else:
                conn.execute(tables.shows.insert(), name=name, group=group, quality=quality,
                             start=start)
            print('Added "[%s] %s"%s starting with episode %s' % (group, name, '' if quality is None else ' (%s)' % quality, start))
        except:
            raise
        finally:
            conn.close()

    def info(self):
        conn = engine.connect()
        for show in conn.execute(select([tables.shows])):
            print('[%s] %s' % (show['group'], show['name']))
            for ep in conn.execute(select([tables.episodes]).where(tables.episodes.c.show_id == show['id'])):
                print('\tepisode %s%s downloaded' % (ep['number'], '' if ep['version'] == 1 else 'v%s' % ep['version']))
        conn.close()

    def process(self):
        print('checking releases')
        conn = engine.connect()
        download_queue = []
        try:
            shows = conn.execute(select([tables.shows]))
            releases = conn.execute(select([tables.releases]).order_by(tables.releases.c.date.desc()))
            for release in releases:
                show = None
                m = re_anime.search(release['name'])
                if m:
                    m = m.groupdict()
                    m['name'] = m['name'].replace('_', ' ').replace('.', ' ')
                    show = conn.execute(select([tables.shows]).where(
                        and_(tables.shows.c.name == m['name'],
                             tables.shows.c.group == m['group']))).fetchone()
                    if show is None:
                        continue
                    episode = conn.execute(select([tables.episodes]).where(
                        and_(tables.episodes.c.show_id == show['id'],
                             tables.episodes.c.number == int(m['episode'])))).fetchone()
                else:
                    continue
                if int(m['episode']) < show['start']:
                    continue  # don't add show

                if not is_release_complete(release['id']):
                    print('incomplete match %s' % release['name'])
                    continue

                if episode is None:
                    # check quality
                    if show['quality'] is not None and m['quality'] != show['quality']:
                        continue
                    # we got what we want! insert!
                    conn.execute(tables.episodes.insert(),
                        show_id=show.id,
                        number=int(m['episode']),
                        version=1 if m['version'] is None else int(m['version'])
                    )
                    download_queue.append(
                        ('queuing [%s] %s - %s%s' % (
                            show['group'], show['name'], m['episode'], '' if m['version'] is None else 'v%s' % m['version']),
                         release['name'], release['id']))
                elif m['version'] is not None and episode['version'] < int(m['version']):
                    conn.execute(tables.episodes.update().where(
                        tables.episodes.c.id == episode['id']).values(
                            version = int(m['version'])))
                    download_queue.append(
                        ('queuing [%s] %s - %s%s' % (
                            show['group'], show['name'], episode['number'], '' if m['version'] is None else 'v%s' % m['version']),
                         release['name'], release['id']))
        except:
            raise
        finally:
            conn.close()
        return download_queue

    def check_match(self, group, name):
        conn = engine.connect()
        releases = conn.execute(select([tables.releases]).order_by(tables.releases.c.date.desc()))
        for release in releases:
            show = None
            m = re_anime.search(release['name'])
            if m and m.group('name') == name and m.group('group') == group:
                print(release['name'], m.groupdict(), is_release_complete(release['id']))
        conn.close()

@click.group()
def cli():
    pass

@cli.command()
@click.argument('group')
@click.argument('name')
@click.option('--quality', default=None, help='quality string to match')
@click.option('--start', default=1, help='First episode to look for')
def add(group, name, quality, start):
    print('adding')
    app = Application()
    app.add_show(group, name, quality=quality, start=start)

@cli.command()
def info():
    app = Application()
    app.info()

@cli.command()
def run():
    app = Application()
    queue = app.process()
    if len(queue) == 0:
        print('no new items found')
        return
    print('got queue with', len(queue), 'items')
    client = SabnzbdClient(settings.SABNZBD_URL, settings.API_KEY)
    for ep in queue:
        print(ep[0])
        nzb = create_nzb(ep[2])
        client.addfile(ep[1], nzb)
        with open('nzb/%s.nzb' % ep[1], 'w') as f:
            f.write(nzb)
    print('done...')

@cli.command()
@click.argument('group')
@click.argument('name')
def check(group, name):
    app = Application()
    app.check_match(group, name)

if __name__ == '__main__':
    cli()
