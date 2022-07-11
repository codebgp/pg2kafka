REGISTRY_NAME?=docker.io/codebgp
IMAGE_VERSION?=0.2.1

pre-commit-run-all:
	GOARCH=amd64 CGO_ENABLED=1 pre-commit run --all-files

install-go-deps:
	script/install-go-deps.sh

test:
	go test ./... --count=1

integration-tests:
	script/tests/go-integration-test.sh

build-docker-image:
	docker build --platform linux/amd64 -t $(REGISTRY_NAME)/pg2kafka:$(IMAGE_VERSION) -f ./Dockerfile .

push-docker-image:
	docker push $(REGISTRY_NAME)/pg2kafka:$(IMAGE_VERSION)
