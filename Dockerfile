# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------

RUN apt install -qy cmake git

RUN git clone https://github.com/gabime/spdlog.git && cd spdlog && mkdir build && cd build &&  cmake .. && make -j && make install

# install compression libs

RUN apt install -qy libzstd-dev liblzo2-dev liblz4-dev libsnappy-dev libbrotli-dev

RUN git clone https://github.com/LLNL/zfp.git && cd zfp && make

RUN apt install -qy clang libboost-all-dev build-essential

RUN mkdir /xdbc-client

RUN rm -rf xdbc-client && mkdir /xdbc-client

COPY xdbc/ /xdbc-client/xdbc/
COPY tests/ /xdbc-client/tests/
COPY CMakeLists.txt /xdbc-client/

# build xdbc
RUN mkdir /xdbc-client/build && cd /xdbc-client/build && cmake .. && make -j8 && make install

# build test
RUN mkdir /xdbc-client/tests/build && cd /xdbc-client/tests/build && cmake .. && make -j8
#WORKDIR /xdbc-client/build

#RUN cmake ..

#RUN make

ENTRYPOINT ["tail", "-f", "/dev/null"]
#------------------------------------------------------------------------


