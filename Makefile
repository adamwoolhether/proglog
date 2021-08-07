CONFIG_PATH=${HOME}/.proglog

.PHONY: init
init:
		mkdir -p ${CONFIG_PATH}

.PHONY: gencert
gencert:
		cfssl gencert \
				-initca test/ca-csr.json | cfssljson -bare ca
		cfssl gencert \
				-ca=ca.pem \
				-ca-key=ca-key.pem \
				-config=test/ca-config.json \
				-profile=server \
				test/server-csr.json | cfssljson -bare server
		mv *.pem *.csr ${CONFIG_PATH}

.PHONY: test
test:
		gotest -race ./... -v

.PHONY: compile
compile:
		protoc 	--go_out=paths=source_relative:. -I. api/v1/*.proto \
				--go-grpc_out=paths=source_relative:. -I. api/v1/*.proto