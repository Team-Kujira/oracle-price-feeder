FROM golang:1.18-buster AS build

ADD . /usr/lib/oracle-price-feeder
WORKDIR /usr/lib/oracle-price-feeder
RUN make install

# locate and copy shared libraries to /deps
# fixes missing libwasmvm.x86_64.so
RUN ldd /go/bin/price-feeder | tr -s '[:blank:]' '\n' | grep '^/' | \
    xargs -I % sh -c 'mkdir -p $(dirname /deps%); cp % /deps%;'

FROM debian:buster

RUN apt-get update && apt-get install ca-certificates -y

COPY --from=build /go/bin/price-feeder /bin/price-feeder
COPY --from=build /deps /

RUN mkdir /root/.kujira
WORKDIR /root/.kujira

CMD price-feeder config.toml