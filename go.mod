module github.com/joincivil/civil-events-processor

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/davecgh/go-spew v1.1.1
	github.com/ethereum/go-ethereum v0.0.0-20190528221609-008d250e3c57
	github.com/fatih/structs v1.1.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/jmoiron/sqlx v0.0.0-20180614180643-0dae4fefe7c0
	github.com/joincivil/civil-events-crawler v0.0.0-20191115212544-903a36715546
	github.com/joincivil/go-common v0.0.0-20191115202858-d23287f7bcb4
	github.com/kelseyhightower/envconfig v1.3.0
	github.com/lib/pq v0.0.0-20180523175426-90697d60dd84
	github.com/pkg/errors v0.8.1
	github.com/robfig/cron v1.2.0
	github.com/shurcooL/graphql v0.0.0-20181231061246-d48a9a75455f // indirect
	google.golang.org/api v0.7.0
)

replace git.apache.org/thrift.git v0.12.0 => github.com/apache/thrift v0.12.0
