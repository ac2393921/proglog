compi;e:
	protoc api/v1/*.proto \
      --go_out=. \
      --go_opt=paths=source_relative \
      --proto_path=.
test:
	go test -v ./...