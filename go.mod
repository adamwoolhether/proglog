module github.com/adamwoolhether/proglog

go 1.15

require (
	github.com/casbin/casbin v1.9.1
	github.com/golang/protobuf v1.5.0
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/memberlist v0.2.4 // indirect
	github.com/hashicorp/raft v1.3.1
	github.com/hashicorp/raft-boltdb v0.0.0-00010101000000-000000000000
	github.com/hashicorp/serf v0.9.5
	github.com/kr/pretty v0.1.0 // indirect
	github.com/soheilhy/cmux v0.1.5
	github.com/stretchr/testify v1.7.0
	github.com/travisjeffery/go-dynaport v1.0.0
	github.com/tysontate/gommap v0.0.0-20210506040252-ef38c88b18e1
	go.opencensus.io v0.23.0
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/sys v0.0.0-20201214210602-f9fddec55a1e // indirect
	golang.org/x/text v0.3.4 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013
	google.golang.org/grpc v1.33.2
	google.golang.org/protobuf v1.26.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v1.0.0
