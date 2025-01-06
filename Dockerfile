# postgres server 14 on ubuntu 22.04 image
FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get upgrade -qy

#-------------------------------------------- Install XDBC and prerequisites -------------------------------------------
# install arrow/parquet dependencies

RUN apt install -qy ca-certificates lsb-release wget

RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

RUN apt update && apt install -qy cmake git gdb nlohmann-json3-dev clang libboost-all-dev build-essential libspdlog-dev iproute2 netcat libarrow-dev libparquet-dev

# install compression libs

RUN apt install -qy libzstd-dev liblzo2-dev liblz4-dev libsnappy-dev libbrotli-dev

RUN git clone https://github.com/lemire/FastPFor.git && cd FastPFor && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . --config Release && \
    make install

RUN git clone https://github.com/LLNL/fpzip.git && cd fpzip && \
    mkdir build && \
    cd build && \
    cmake .. && \
    cmake --build . --config Release && \
    make install


RUN mkdir /xdbc-client

RUN rm -rf xdbc-client && mkdir /xdbc-client

COPY xdbc/ /xdbc-client/xdbc/
COPY Sinks/ /xdbc-client/Sinks/
COPY tests/ /xdbc-client/tests/
COPY tests/schemas/ /xdbc-client/tests/schemas/
COPY CMakeLists.txt /xdbc-client/
COPY optimizer /xdbc-client/

# build xdbc
RUN rm -rf  /xdbc-client/CMakeCache.txt
RUN mkdir /xdbc-client/build && cd /xdbc-client/build && cmake .. -D CMAKE_BUILD_TYPE=Release && make -j8 && make install
#RUN mkdir /xdbc-client/build && cd /xdbc-client/build && cmake .. && make -j8 && make install

# build test
#RUN mkdir /xdbc-client/tests/build && cd /xdbc-client/tests/build && cmake .. -D CMAKE_BUILD_TYPE=Release && make -j8
RUN mkdir /xdbc-client/Sinks/build && cd /xdbc-client/Sinks/build && cmake .. -D CMAKE_BUILD_TYPE=Release && make -j8

#ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
RUN ldconfig
#WORKDIR /xdbc-client/build
#RUN cmake ..

#RUN make

ENTRYPOINT ["tail", "-f", "/dev/null"]
#------------------------------------------------------------------------


