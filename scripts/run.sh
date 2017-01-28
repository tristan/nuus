#!/bin/bash
sudo systemctl start mysqld.service
LC_ALL=en_US.UTF-8
LANG=en_US.UTF-8
if [ -z $APP ]; then
    APP=/home/tristan/projects/nuus
fi
cd $APP
source env/bin/activate
echo "======" `date` "======" >>output.log
python nuus/indexer/downloader.py a.b.multimedia.anime.highspeed >>output.log 2>&1
parse_result=1
while [ $parse_result -ne 0 ]; do
    python nuus/indexer/parser.py >>output.log 2>&1
    parse_result=$?
done
python nuus/pvr.py run >>output.log 2>&1
deactivate
sudo systemctl stop mysqld.service
