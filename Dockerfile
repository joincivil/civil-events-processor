#FROM alpine:3.7
FROM golang:1.11.1
ADD build build
ADD build/processor /processor
RUN chmod u+x /processor

CMD ["/processor", "-logtostderr=true", "-stderrthreshold=INFO"]
