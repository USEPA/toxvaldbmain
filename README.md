# toxvaldbmain

# Background
ToxValDB is a large compilation of in vivo toxicology data and risk assessment values. The database originated in response to the need for consistently annotated and computable toxicology data for use in the development and validation of non-animal new approach methods (NAMs). The database has two major components. The first, ToxValDB Stage contains data that closely match data from each source, in both structure and terminology. The second (the main ToxValDB database) maps all source data to a consistent structure and set of vocabularies. The current version of the database (9.7.0) contains 249,071 records covering 41,855 chemicals (27,068 with defined chemical structure) from 51 sources (74 source tables).

# Repository Content
This repository contains the R scripts used to generate the "Main" database for ToxValDB. Note, the code provided is for publication transparency sake only. Users are not meant to run the code and reproduce the database themselves. The generation of the database is a semi-automated process, so it is not possible to completely reproduce the database from running the provided code alone. Please access the complete database data from the links provided below.

# Where to access ToxValDB data
- [CompTox Chemicals Dashboard](https://comptox.epa.gov/dashboard/)
	 - Navigate to the "Hazard Data" sidebar tab from a chemical page
- [US EPA FigShare Dataset](https://doi.org/10.23645/epacomptox.20394501)
	- Versioned releases of ToxValDB in XLSX and MySQL dump file format with associated documentation
	- Note, the FigShare DOI link will land on the most recent version of the FigShare posting. Use the version dropdown menu to navigate to the desired release version based on the dataset title.
	- Version v9.7.0 was also released as a [Zenodo Dataset](https://zenodo.org/records/17088058)

# Repository Links
- [toxvaldbstage](https://github.com/usepa/toxvaldbstage)
- [toxvaldbmain](https://github.com/usepa/toxvaldbmain/)

# Contribute to ToxValDB
ToxValDB is under continuous development with a focus on improving the current data model and continuing to expand the number of sources. We encourage users to provide feedback on ToxValDB and to help identify new datasets. Please contact Taylor Wall (wall.taylor@epa.gov) or Chelsea Weitekamp (weitekamp.chelsea@epa.gov) with feedback or potential datasets to add to ToxValDB.

# Disclaimer
The United States Environmental Protection Agency (EPA) GitHub project code is provided on an "as is" basis and the user assumes responsibility for its use.  EPA has relinquished control of the information and no longer has responsibility to protect the integrity , confidentiality, or availability of the information.  Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by EPA.  The EPA seal and logo shall not be used in any manner to imply endorsement of any commercial product or activity by EPA or the United States Government.
