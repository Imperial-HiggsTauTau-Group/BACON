# BACON

B.A.C.O.N. - Bulk Automated Copying and Operations Nexus

Welcome to B.A.C.O.N., the tastiest way to handle your high-energy physics data transfers! 🍖

Do you have a hunger for efficiency in file management? Craving a seamless, automated solution to copy and organize your data across CERN's tiered sites? Look no further! B.A.C.O.N. is here to serve up the perfect slice of technology you need.

## Run3_2024

For previous years, the ``.yaml`` files under ``samples`` can be processed in a relatively straightforward way, allowing for the paths of the files we want to copy over to be readily extracted with a ``gfal-ls`` call inside ``davs://eoscms.cern.ch/eos/cms/store``. Run3_2024 samples are not stored here and follow a different naming convention, meaning the method by which we extract the list of file paths has to be different. The difference in naming convention of the directories under which our desired samples are stored means that currently the path to each sample has to effectively be specified "by hand" under ``utils/sample_dir_2024.json``.

Furthermore, the fact that the ``Run3_2024`` samples are not stored under ``davs://`` (but rather ``root://``) means ``fts-rest-transfer-submit`` no longer works for copying files over to the Imperial ``davs://``. As such, B.A.C.O.N. has been modified such that if ``submit_jsons.py`` is run with the flag ``--year Run3_2024``, then the script instead attempts to submit ``gfal-copy`` commands via HTCondor to complete the desired transfers. This was designed for use on Imperial's lx-machines rather than CERN's LXPLUS. For this to work, one must have a valid grid certificate, and have correctly initialised and exported it, e.g.: 

```
source /vols/grid/cms/setup.sh
voms-proxy-init --rfc --voms cms --valid 192:00 --out ${HOME}/cms.proxy
export X509_USER_PROXY=${HOME}/cms.proxy
```
