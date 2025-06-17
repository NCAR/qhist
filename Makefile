PREFIX ?= /usr/local
VERSION := 1.1

make install: lib/pbsparse/Makefile
	mkdir -p $(PREFIX)/bin $(PREFIX)/lib/qhist $(PREFIX)/share
	sed 's|/src|/lib/qhist|' bin/qhist > $(PREFIX)/bin/qhist
	cp -r src/qhist $(PREFIX)/lib/qhist
	cp -r lib/pbsparse/src/pbsparse $(PREFIX)/lib/qhist
	cp -r share $(PREFIX)/share
	chmod +x $(PREFIX)/bin/qhist

lib/pbsparse/Makefile:
	git submodule init
	git submodule update

build:
	python3 -m build

# These commands can only be run successfully by package maintainers
manual-upload: 
	python3 -m twine upload dist/*

test-upload:
	python3 -m twine upload --repository testpypi dist/*

#release: man
#	git tag v$(VERSION)
#	git push origin v$(VERSION)

# Requires packages "pbsparse" and "argparse-manpage" in your Python environment
man:
	argparse-manpage --pyfile src/qhist/qhist.py --author "Written by Brian Vanderwende."   \
		--project-name qhist --function get_parser --version $(VERSION)                     \
		--description "a utility for querying historical PBS records"                       \
		--manual-title "PBS Professional Community Utilities"                               \
		--output share/man/man1/qhist.1

clean:
	rm -rf dist build
