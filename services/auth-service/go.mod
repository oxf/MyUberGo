module auth-service

go 1.26

require (
	github.com/golang-jwt/jwt/v5 v5.0.0
	github.com/lib/pq v1.10.7
	github.com/oxf/MyUber/contracts v0.0.0
	github.com/sirupsen/logrus v1.9.4
	golang.org/x/crypto v0.11.0
)

require golang.org/x/sys v0.13.0 // indirect

replace github.com/oxf/MyUber/contracts => ../contracts
