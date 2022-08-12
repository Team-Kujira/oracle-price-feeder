FROM golang:1.18-buster

WORKDIR /usr/lib
RUN git clone https://github.com/Team-Kujira/core.git
ADD . oracle-price-feeder
WORKDIR /usr/lib/oracle-price-feeder
RUN make install

RUN mkdir /root/.kujira
WORKDIR /root/.kujira

CMD price-feeder config.toml