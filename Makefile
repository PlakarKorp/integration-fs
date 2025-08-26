GO=go
PLAKAR=../plakar/plakar

all: build
	make -C ../plakar
	rm -f ./fis_v1.0.0_openbsd_amd64.ptar
	${PLAKAR} pkg rm fis || true
	${PLAKAR} pkg create ./manifest.yaml
	${PLAKAR} pkg add ./fis_v1.0.0_openbsd_amd64.ptar
	${PLAKAR} backup fis://$$PWD

build:
	${GO} build -v -o fs-importer ./plugin/importer
	${GO} build -v -o fs-exporter ./plugin/exporter
	${GO} build -v -o fs-storage ./plugin/storage

clean:
	rm -f fs-importer fs-exporter fs-storage fs-*.ptar
