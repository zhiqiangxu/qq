#!/bin/bash

# You might need to go get -v github.com/gogo/protobuf/...

pushd "$(dirname "$0")"

protoc --gogofast_out=. qq.proto 

popd