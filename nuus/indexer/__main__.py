from nuus.indexer import downloader, parser
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

if __name__ == '__main__':
    groups = sys.argv[1:]
    for group in groups:
        downloader.run(group)
