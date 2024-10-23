FROM golang

WORKDIR /chotki

COPY . /chotki

ENTRYPOINT make test


