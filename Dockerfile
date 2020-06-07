FROM golang:1.13.8

#RUN apt-get update && apt-get install -y mongodb nginx

WORKDIR $GOPATH/src/github.com/columbustech/profiler

COPY api .

CMD ./profiler

#CMD ["sh", "-c", "tail -f /dev/null"]
