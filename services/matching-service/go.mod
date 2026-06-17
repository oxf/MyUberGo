module matching-service

go 1.26

require (
	github.com/google/uuid v1.6.0
	github.com/oxf/MyUber/contracts v0.0.0
	github.com/segmentio/kafka-go v0.4.28
	github.com/lib/pq v1.10.7
)

require (
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.9.8 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
)

replace github.com/oxf/MyUber/contracts => ../contracts
