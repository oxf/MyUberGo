module matching-service

go 1.26

require (
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.7
	github.com/oxf/MyUber/contracts v0.0.0
	github.com/redis/go-redis/v9 v9.0.0
	github.com/segmentio/kafka-go v0.4.28
	github.com/sirupsen/logrus v1.8.1
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.9.8 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/sys v0.0.0-20191026070338-33540a1f6037 // indirect
)

replace github.com/oxf/MyUber/contracts => ../contracts
