.PHONY: build
build:
	dune build zarr zarr-lwt zarr-sync zarr-eio

.PHONY: clean
clean:
	dune clean

.PHONY: test
test: build
	dune runtest --force

.PHONY: test-cov
test-cov: build
	OUNIT_CI=true dune runtest --instrument-with bisect_ppx --force
	bisect-ppx-report summary --per-file

.PHONY: show-cov
show-cov: test-cov
	bisect-ppx-report html
	chromium _coverage/index.html

docs:
	dune build @doc

.PHONY: view-docs
view-docs: docs
	chromium _build/default/_doc/_html/index.html 

.PHONY: minio
minio:
	mkdir -p /tmp/minio/test-bucket-lwt
	docker run --rm -it -p 9000:9000 -v /tmp/minio:/minio minio/minio:latest server /minio
