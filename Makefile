PREFIX ?= /usr/local

make install: lib/pbsparse/Makefile
	mkdir -p $(PREFIX)/bin $(PREFIX)/lib/qhist $(PREFIX)/share
	sed 's|/src|/lib/qhist|' bin/qhist > $(PREFIX)/bin/qhist
	cp -r src/qhist $(PREFIX)/lib/qhist
	cp -r lib/pbsparse/src/pbsparse $(PREFIX)/lib/qhist
	cp -r man $(PREFIX)/share
	chmod +x $(PREFIX)/bin/qhist

lib/pbsparse/Makefile:
	git submodule init
	git submodule update

build:
	python3 -m build

upload:
	python3 -m twine upload dist/*

test-upload:
	python3 -m twine upload --repository testpypi dist/*

clean:
	rm -rf dist build
