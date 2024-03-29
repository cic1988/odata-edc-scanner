# Custom Scanner: odata-edc-scanner
Odata scanner for EDC

# Notice
The main branch is since 06.06.2022 set to v2 branch as v2 is the main supported Odata version.

# Overview
:white_check_mark: (supported) :construction: (on-going) :soon: (plan) :no_entry_sign: (backlog)

* V2 Objects :white_check_mark: (see v2 and main branch)
  * :white_check_mark: basic authentication
  * :white_check_mark: entity type
  * :white_check_mark: key
  * :white_check_mark: property
  * :white_check_mark: navigation property
  * :white_check_mark: association + navigation property
  * :white_check_mark: function import
  * :white_check_mark: custom attributes

* V3 Objects :no_entry_sign:

* V4 Objects :construction: (see v4 branch)
  * :no_entry_sign: basic authentication
  * :white_check_mark: entity type
  * :white_check_mark: key
  * :white_check_mark: property
  * :white_check_mark: navigation property
  * :no_entry_sign: association + navigation property
  * :no_entry_sign: function import
  * :no_entry_sign: custom attriburtes

* Feature
  * :white_check_mark: metadata
  * :white_check_mark: lineage
  * :white_check_mark: profiling (configurable + filter) + data domain discovery
  * :warning: currently only basic authentication supported by v2 or main branch


* Screenshot (example of https://services.odata.org/V2/Northwind/Northwind.svc)

| Screenshot 1 | Screenshot 2 |
| ------------- | ------------- |
| ![image](https://user-images.githubusercontent.com/7901026/147205262-58637155-00c9-41bf-ad11-818fd2b3a7ff.png)  | ![image](https://user-images.githubusercontent.com/7901026/147205341-829e5e96-c531-4e95-b54b-67c4eac0e8e9.png)  |
| ![image](https://user-images.githubusercontent.com/7901026/147205890-27703e97-e0a8-440d-90a4-80b819c67789.png)  | ![image](https://user-images.githubusercontent.com/7901026/147392282-99236022-79f6-4c44-b3ba-4ba683702e93.png) |
| ![image](https://user-images.githubusercontent.com/7901026/148360223-eef3475a-fa0e-4fad-a747-52f3581a4152.png) | ![image](https://user-images.githubusercontent.com/7901026/148360332-fba2337b-0891-4250-ac47-3540196094f5.png) |


# Usage

### Import model
1. Download the model.zip file from the latest release: https://github.com/cic1988/odata-edc-scanner/releases
2. Import this model in EDC admin panel
<img src="https://user-images.githubusercontent.com/7901026/148257997-6bade4ae-1dbf-4e95-baeb-37eaef226033.png" data-canonical-src="https://user-images.githubusercontent.com/7901026/148257997-6bade4ae-1dbf-4e95-baeb-37eaef226033.png" width="300" />
3. Create a custom resource with imported data model
<img src="https://user-images.githubusercontent.com/7901026/148259028-eccde493-adc9-4f35-a5f7-202d9a22e891.png" data-canonical-src="https://user-images.githubusercontent.com/7901026/148259028-eccde493-adc9-4f35-a5f7-202d9a22e891.png" width="300" />
4. Use this newly create resource to setup scanner

### Initial setup

#### Option One: if running the build binary:
1. Download the binary files from the latest release (currently CentOS/Redhat and OSX are supported): https://github.com/cic1988/odata-edc-scanner/releases
2. Unpack the zip file in a target folder
3. `chmod u+x odata-edc-scanner`
4. `cp config.ini.sample config.ini`
5. Edit the config.ini with required information (see comment in the file)
6. Run the command `./odata-edc-scanner -f`

#### Option Two: if running the python source code:
1. Clone this repository via `git clone https://github.com/cic1988/odata-edc-scanner.git`
2. (optional) `yum install python3-devel -y`
3. `cd odata-edc-scanner`
4. `python3 -m venv . `
5. `source bin/activate`
6. `pip install --upgrade pip`
7. `pip install -r requirements.txt`
8. `cp config.ini.sample config.ini`
9. Edit the config.ini with required information (see comment in the file)
10. `chmod u+x main.py`

### Generate the needed files manually (if you want to run scanner off-EDC):
1. Run `./main.py -f`
2. Upload the metadata.zip to the scanner configuration
3. Upload the ProfileableClassTypes.csv to the field `Classes enabled for Profiling`
4. (optional - when profiling enabled) Point the SFTP location and login to the folder, where the script has created the files

### Example configuration in EDC (if you want to automate scanning in EDC):
| Step | Screenshot |
| ------------- | ------------- |
| General setup. for example /home/infa/custom_scanner/odata/odata-edc-scanner/ is where you have cloned the repository. ODATASAMPLE is the folder specified in the config.ini file | ![image](https://user-images.githubusercontent.com/7901026/147973934-017ab4f3-8e86-4cdc-b431-8b7d37421071.png) |
| Upload the generated ProfileableClassTypes.csv file | ![image](https://user-images.githubusercontent.com/7901026/147974373-f8695824-b5a1-405e-8ef7-89c466e0c1fa.png) |
| SFTP configuration for the generated profiling files | ![image](https://user-images.githubusercontent.com/7901026/147974477-c5ba95d0-364f-4602-851f-5114f04f9727.png)|

Other standard scanner configurations (data domains, etc.) are similar here. Then run the job.




