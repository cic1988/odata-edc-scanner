#!/bin/sh

source bin/activate
pip install --upgrade pip
pip install -r requirements.txt

rm -rf ./dist/*

pyinstaller -F main.py --name="odata-edc-scanner" --add-binary=`which redis-server`:bin --clean
pyinstaller odata-edc-scanner.spec

# TODO: some files are just not really copied ...
cp `which redis-server` ./dist/
cp config.ini.sample ./dist/
