
# default usenet settings
USENET_MAX_CONNECTIONS=6

# database settings
DATABASE_HOST='localhost'
DATABASE_DATABASE='nuus'
DATABASE_PASSWORD=''
DATABASE_USER='mysql'

# cache directory
import os
CACHE_BASE=os.path.join('cache')
CACHE_INBOX=os.path.join('cache','in')
CACHE_COMPLETE=os.path.join('cache','complete')

CACHE_FILE_FORMAT = '{group}-{page}.{status}.gz'
CACHE_FILE_REGEX = '^(?P<group>[a-zA-Z0-9\.]+)-(?P<page>\d+)\.(?P<status>\w+)\.gz$'

CACHE_LINE_FORMAT = '{article_id}\t{subject}\t{poster}\t{date}\t{size}\n'
CACHE_LINE_REGEX = '^(?P<article_id>[^\t]+)\t(?P<subject>[^\t]+)\t(?P<poster>[^\t]+)\t(?P<date>\d+)\t(?P<size>\d+)\n$'
