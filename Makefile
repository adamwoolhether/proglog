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
	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=client \
			test/client-csr.json | cfssljson -bare client

	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=client \
			-cn="root" \
			test/client-csr.json | cfssljson -bare root-client

	cfssl gencert \
			-ca=ca.pem \
			-ca-key=ca-key.pem \
			-config=test/ca-config.json \
			-profile=client \
			-cn="nobody" \
			test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test:	$(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
		gotest -race ./... -v

.PHONY: compile
compile:
		protoc 	--go_out=paths=source_relative:. -I. api/v1/*.proto \
				--go-grpc_out=paths=source_relative:. -I. api/v1/*.proto