#FROM alpine:3.7
FROM golang:1.10
ADD build build
ADD build/processorcron /processorcron
RUN chmod u+x /processorcron

CMD ["/processorcron", "-logtostderr=true", "-stderrthreshold=INFO"]
