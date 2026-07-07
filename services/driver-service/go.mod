module driver-service

go 1.26

require (
	github.com/lib/pq v1.10.7
	github.com/oxf/MyUber/contracts v0.0.0
	github.com/sirupsen/logrus v1.9.4
)

require golang.org/x/sys v0.13.0 // indirect

replace github.com/oxf/MyUber/contracts => ../contracts
