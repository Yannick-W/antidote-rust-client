PB_OUT_DIR=src
PB_OUT_FILE=$(PB_OUT_DIR)\antidote_pb.rs
ANTIDOTE_PB=protos\antidote_pb.proto

## Windows cmd

gen:
	protoc --rust_out $(PB_OUT_DIR) $(ANTIDOTE_PB)

clean:
	if exist $(PB_OUT_FILE) del /q $(PB_OUT_FILE)
	cargo clean

docker:
	cmd.exe && docker-compose up


