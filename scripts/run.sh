#!/bin/bash
LC_ALL=en_US.UTF-8
LANG=en_US.UTF-8
if [ -z $APP ]; then
    APP=/home/tristan/projects/nuus
fi
cd $APP
source env/bin/activate
echo "======" `date` "======" >>output.log
python nuus/indexer/downloader.py a.b.multimedia.anime.highspeed >>output.log 2>&1
python nuus/indexer/parser.py >>output.log 2>&1
python nuus/pvr.py run >>output.log 2>&1
deactivate
