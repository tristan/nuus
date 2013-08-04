
# default usenet settings
USENET_MAX_CONNECTIONS=6

# database settings
DATABASE_HOST='localhost'
DATABASE_DATABASE='nuus'
DATABASE_PASSWORD=''
DATABASE_USER='mysql'

# cache directory
import os
BLOCK_STORAGE_DIR='storage'

BLOCK_FILE_FORMAT = '{group}.{start}-{end}.gz'
BLOCK_FILE_REGEX = '^(?P<group>[a-zA-Z0-9\.]+)\.(?P<start>\d+)-(?P<end>\d+)\.gz$'
