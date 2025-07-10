GO = go

all:
	${GO} build -v -o fs-importer ./importer/
	${GO} build -v -o fs-exporter ./exporter/
	${GO} build -v -o fs-store    ./store/
