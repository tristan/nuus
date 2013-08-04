# Nuus

## setup

    virtualenv .
    source bin/activate # or on windows: scripts\activate.bat
    pip install flask sqlalchemy mysql-connector-python
    python -c "import os; f = open(os.path.abspath(os.path.join(os.__file__, '..', 'site-packages', 'nuus.pth')), 'w'); f.write(os.path.abspath('.')); f.close()"

## run server

    python nuus

## database setup

configure db settings in `settings.py`

    DATABASE_USER # defaults to 'mysql'
    DATABASE_HOST # defaults to 'localhost'
    DATABASE_PASSWORD # defaults to ''
    DATABASE_NAME # defaults to 'nuus'

run the following:

    mysql -u {user}
    create database nuus default character set = 'utf8' default collate = 'utf8_general_cs';
    python nuus/database/tables.py

## configure indexer and parser

The indexer reads a file `groups.txt` to know which groups to process. create this file and add the groups (one per line) you want to index.

The parser reads the output of the indexer, and parses each subject using regular expressions from `patterns.txt`.

Each pattern should use named groups (e.g. `(?P<file_name>\w+)`) and include group matches for the following:

| Group Name      | Description                               | Optional |
| --------------- | ----------------------------------------- | -------- |
| `release_name`  | the name of the scene release             | yes      |
| `release_part`  | the part number for the release           | yes      |
| `release_total` | the total number of parts for the release | yes      |
| `file_name`     | the file name                             | no       |
| `file_part`     | the part number for the file              | no       |
| `file_total`    | the total number of parts for the file    | no       |

Note: if any of the release groups are not matched, the file will not be made part of a release (but will still be searchable as a single file).

## indexer

    python indexer.py