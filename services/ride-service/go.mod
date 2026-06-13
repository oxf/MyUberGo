module ride-service

go 1.23

require (
	github.com/golang-jwt/jwt/v5 v5.0.0
	github.com/lib/pq v1.10.7
	github.com/oxf/MyUber/contracts v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.28
)

require (
	github.com/golang/snappy v0.0.1 // indirect
	github.com/klauspost/compress v1.9.8 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
)

replace github.com/oxf/MyUber/contracts => ../contracts
