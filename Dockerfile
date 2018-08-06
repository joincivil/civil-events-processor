#FROM alpine:3.7
FROM golang:1.10
ADD build build
ADD build/processor /processor
RUN chmod u+x /processor

CMD ["/processor"]
