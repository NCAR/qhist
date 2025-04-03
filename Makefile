PREFIX ?= /usr/local

make install:
	echo $(PREFIX)

build:
	python3 -m build

upload:
	python3 -m twine upload dist/*

test-upload:
	python3 -m twine upload --repository testpypi dist/*

clean:
	rm -rf dist build
