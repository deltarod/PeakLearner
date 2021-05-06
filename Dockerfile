FROM ubuntu as envSetup
ENV TZ="America/Phoenix"
RUN apt-get update
RUN apt-get -y install dialog apt-utils ca-certificates
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata
ENV TMPDIR="/home/tem83/tmp/"
RUN apt-get install -y samtools libdb5.3-dev libdb5.3++-dev git build-essential zlib1g-dev libxml2-dev libexpat-dev npm nano python3-numpy python3-pip db-util
# libgfortran3 is not included in Ubuntu20.04 but required for current casa6-py36 whl
# here we copy .so filed from Ubuntu18.04
#   reference: https://pkgs.org
RUN apt-get install -y wget && \
    wget http://archive.ubuntu.com/ubuntu/pool/universe/g/gcc-6/libgfortran3_6.4.0-17ubuntu1_amd64.deb && \
    dpkg-deb -c libgfortran3_6.4.0-17ubuntu1_amd64.deb && \
    dpkg-deb -R libgfortran3_6.4.0-17ubuntu1_amd64.deb / && \
    rm -rf ./libgfortran3_6.4.0-17ubuntu1_amd64.deb /DEBIAN
RUN mkdir /build/
WORKDIR /build/
RUN mkdir PeakLearner/
WORKDIR PeakLearner/
RUN mkdir bin/
ADD http://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64.v385/bigWigSummary bin/
RUN chmod a+x bin/bigWigSummary

FROM envSetup AS jbrowse
RUN mkdir jbrowse/
WORKDIR jbrowse/
RUN git clone https://github.com/PeakLearner/jbrowse.git
WORKDIR jbrowse/
RUN git submodule update --init --remote
RUN ./setup.sh
WORKDIR ../../

FROM jbrowse AS pythonSetup
COPY ./requirements.txt .
RUN python3 -m pip install -U pip && \
    python3 -m pip install -r requirements.txt

FROM pythonSetup AS build
COPY . .
RUN python3 -m pip install -e .

FROM build AS profile
RUN mkdir testProfile/
CMD ["uwsgi", "profile.ini"]

FROM build AS run
CMD ["uwsgi", "wsgi.ini"]