pre-commit-run-all:
	GOARCH=amd64 CGO_ENABLED=1 pre-commit run --all-files

install-go-deps:
	script/install-go-deps.sh

test:
	go test ./... --count=1

integration-tests:
	script/tests/go-integration-test.sh
