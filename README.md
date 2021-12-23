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

* Feature
  * metadata
  * lineage
  * profiling (configurable) + data domain discovery
 
* Screenshot
 
![image](https://user-images.githubusercontent.com/7901026/147205262-58637155-00c9-41bf-ad11-818fd2b3a7ff.png)
![image](https://user-images.githubusercontent.com/7901026/147205341-829e5e96-c531-4e95-b54b-67c4eac0e8e9.png)
![image](https://user-images.githubusercontent.com/7901026/147205890-27703e97-e0a8-440d-90a4-80b819c67789.png)


# Usage
1. Clone this repository via `git clone https://github.com/cic1988/odata-edc-scanner.git`
2. `cd odata-edc-scanner`
3. `python3 -m venv . `
4. `pip install -r requirements.txt`
5. `cp config.ini.sample config.ini`
6. Edit the config.ini with required information (see comment in the file)
7. `./main.py -f`
