# Project: odata-edc-scanner
Odata scanner for EDC

# Overview
:white_check_mark: (supported) :construction: (on-going) :soon: (plan)

* Objects (v2 only)
  * :white_check_mark: entity type
  * :white_check_mark: key
  * :white_check_mark: property
  * :white_check_mark: navigation property
  * :construction: association + navigation property
  * :soon: function import
  * :soon: custom attributes


# Usage
1. Clone this repository via `git clone https://github.com/cic1988/odata-edc-scanner.git`
2. `cd odata-edc-scanner`
3. `python3 -m venv . `
4. `pip install -r requirements.txt`
5. `cp config.ini.sample config.ini`
6. Edit the config.ini with required information (see comment in the file)
7. `./main.py -f`
