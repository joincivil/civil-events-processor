module github.com/joincivil/civil-events-processor

go 1.12

require (
	cloud.google.com/go v0.43.0
	github.com/apilayer/freegeoip v3.5.0+incompatible // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/docker v1.13.1 // indirect
	github.com/ethereum/go-ethereum v1.9.6
	github.com/fatih/structs v1.1.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/howeyc/fsnotify v0.9.0 // indirect
	github.com/influxdata/influxdb v1.7.7 // indirect
	github.com/jmoiron/sqlx v0.0.0-20180614180643-0dae4fefe7c0
	github.com/joincivil/civil-events-crawler v0.0.0-20200102204429-5e951e46939f
	github.com/joincivil/go-common v0.0.0-20200102202009-410bce4f4336
	github.com/karalabe/hid v1.0.0 // indirect
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v0.0.0-20180523175426-90697d60dd84
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oschwald/maxminddb-golang v1.3.1 // indirect
	github.com/pkg/errors v0.8.1
	github.com/robfig/cron v1.2.0
	github.com/shurcooL/graphql v0.0.0-20181231061246-d48a9a75455f // indirect
	github.com/vektah/gorunpkg v0.0.0-20190126024156-2aeb42363e48 // indirect
	golang.org/x/crypto v0.0.0-20191219195013-becbf705a915 // indirect
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553 // indirect
	golang.org/x/sys v0.0.0-20191220142924-d4481acd189f // indirect
	golang.org/x/tools v0.0.0-20191219230827-5e752206af05 // indirect
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543 // indirect
	google.golang.org/api v0.7.0
)

replace git.apache.org/thrift.git v0.12.0 => github.com/apache/thrift v0.12.0

replace github.com/btcsuite/btcd v0.20.0-beta => github.com/btcsuite/btcd v0.20.1-beta
