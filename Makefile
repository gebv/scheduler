vendor:
	go get -v github.com/stretchr/testify/assert \
		gopkg.in/vmihailenco/msgpack.v2 \
		github.com/satori/go.uuid \
		github.com/boltdb/bolt \
		github.com/streadway/amqp 
.PHONY: vendor
test: vendor
	go test -v \
		-bench=. -benchmem \
		-run=. \
		./...
.PHONY: test