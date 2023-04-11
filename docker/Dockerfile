# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

RUN apt install -qy clang libboost-all-dev libzstd-dev build-essential

RUN apt install -qy cmake

RUN mkdir /xdbc-client

COPY "./../CMakeLists.txt" /xdbc-client/CMakeLists.txt
COPY "./../tests" /xdbc-client/tests
COPY "./../xdbc" /xdbc-client/xdbc

RUN mkdir /xdbc-client/build

WORKDIR /xdbc-client/build

RUN cmake ..

RUN make

#------------------------------------------------------------------------


