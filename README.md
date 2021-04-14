# PeakLearner
![](https://travis-ci.com/PeakLearner/PeakLearner.svg?branch=master)

An Interactive Labeling and model generation system for genomic data.

## Features

UCSCToPeakLearner: A custom dropdown menu which can take a hub.txt file, parse, and create a new data directory for JBrowse to read from.

Interactive Labeling: A user can create, modify, and delete labels for a given track. These labels are then stored on the webserver

## Setup
This section will explain the setup for the different systems of PeakLearner

Both sections were developed on Python 3.8, they both should work with this version.
The Web Server might work with 3.7 but this is untested.
The Slurm Server should work with 3.6 and up but this is also untested. 

### Web Server
1. `git clone https://github.com/deltarod/PeakLearner.git`
2. `cd PeakLearner/`
3. `mkdir bin`
4. `cd bin/`
5. `wget http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64.v385/bigWigSummary`
6. `sudo chmod a+x bigWigSummary`
7. `cd ..`
8. `git submodule update --init --recursive` - Initializes recursive submodules
9. `sudo apt update && sudo apt install samtools libdb5.3-dev`
10. `python3 -m pip install -r requirements.txt` - Installs Python Requirements
11. `cd jbrowse/jbrowse`
12. `./setup.sh`
13. `cd ..`
14. `uwsgi wsgi.ini`
The PeakLearner + Jbrowse webserver should now be started, and can be access at 127.0.0.1:8080.
For use in deployment, 

### Slurm Server
1. `git clone https://github.com/PeakLearner/PeakLearner.git`
2. `cd PeakLearner/`
3. `mkdir bin`
4. `cd bin/`
5. `wget http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64.v385/bigWigToBedGraph`
6. `sudo chmod a+x bigWigToBedGraph`
7. `cd ../server/`
8. `sudo apt install r-base`
9. `sudo Rscript -e 'install.packages("data.table")'`
10. `python3 run.py` - This will generate the intial config
11. Setup PeakLearnerSlurm.cfg, for more information see the [configuration section](#configuration)


## Using Podman
1. `git clone https://github.com/PeakLearner/PeakLearner.git`
2. `cd PeakLearner/`
3. `podman build -t pl .`
4. `podman run --name PeakLearner -dp [Output Port]:8080 -v /dir/to/data/:/build/PeakLearner/jbrowse/jbrowse/data/ pl`

## Configuration
This section will explain some of the configuration of the different systems

### Web Server Configuration

#### Config File
- HTTP Section: Info about the http server
    - port: port it will be served on
    - path: path to jbrowse files (typically cloned during web server setup, default will work)
- data: Info about data within jbrowse folder
    - path: path to data folder, default will work


### Slurm Configuration
TBD

## Todos
1. Authentication system
2. Customizable Hub Upload
3. Better CI

## Contributing
Follow the [Contributing Guide](CONTRIBUTING.md)!


# References

Jbrowse Documentation               https://jbrowse.org/docs/installation.html

gmod wiki                           http://gmod.org/wiki/JBrowse

Jbrowse git                         https://github.com/GMOD/jbrowse/

PeakLearner git                     https://github.com/deltarod/PeakLearner

MultiBigWig git                     https://github.com/elsiklab/multibigwig

WiggleHighlighter git               https://github.com/cmdcolin/wigglehighlighter/issues/1

InteractivePeakAnnotator git        https://github.com/cmdcolin/interactivepeakannotator

PeakLearnerPlugin git               https://github.com/deltarod/PeakLearnerPlugin

PeakError git                       https://github.com/deltarod/PeakError
