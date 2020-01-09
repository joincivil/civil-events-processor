module github.com/joincivil/civil-events-processor

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/davecgh/go-spew v1.1.1
	github.com/ethereum/go-ethereum v1.9.6
	github.com/fatih/structs v1.1.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/jmoiron/sqlx v0.0.0-20180614180643-0dae4fefe7c0
	github.com/joincivil/civil-events-crawler v0.0.0-20200107003832-d536ba1f7b6f
	github.com/joincivil/go-common v0.0.0-20200107002045-7da72c934006
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v0.0.0-20180523175426-90697d60dd84
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pkg/errors v0.8.1
	github.com/robfig/cron v1.2.0
	github.com/shurcooL/graphql v0.0.0-20181231061246-d48a9a75455f // indirect
	golang.org/x/crypto v0.0.0-20191219195013-becbf705a915 // indirect
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553 // indirect
	golang.org/x/sys v0.0.0-20191220142924-d4481acd189f // indirect
	google.golang.org/api v0.7.0
)

replace git.apache.org/thrift.git v0.12.0 => github.com/apache/thrift v0.12.0

replace github.com/btcsuite/btcd v0.20.0-beta => github.com/btcsuite/btcd v0.20.1-beta
