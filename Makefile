# full bash shell requied for our complex make rules
.ONESHELL:
SHELL := /bin/bash
CONDA_ROOT := $(shell conda info --base)

# common way to inialize enviromnent across various types of systems
config_env := module load conda >/dev/null 2>&1 || true && . $(CONDA_ROOT)/etc/profile.d/conda.sh

PREFIX ?= /usr/local
VERSION := 1.1

install: lib/pbsparse/Makefile
	mkdir -p $(PREFIX)/bin $(PREFIX)/lib/qhist
	sed 's|/src|/lib/qhist|' bin/qhist > $(PREFIX)/bin/qhist
	cp -r src/qhist $(PREFIX)/lib/qhist
	cp -r lib/pbsparse/src/pbsparse $(PREFIX)/lib/qhist
	cp -r share $(PREFIX)/share
	chmod +x $(PREFIX)/bin/qhist

$(PREFIX)/bin/qhist:
	@echo "You must run 'make install' before you can install any extensions"
	@exit 1

ncar-extensions: $(PREFIX)/bin/qhist
	git clone https://github.com/NCAR/pbs-parser-ncar.git
	cp pbs-parser-ncar/ncar.py $(PREFIX)/lib/qhist/qhist/extensions/

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

%: %.yaml
	[ -d $@ ] && mv $@ $@.old && rm -rf $@.old &
	$(config_env)
	conda env create --file $< --prefix $@
	conda activate ./$@
	conda list
	pipdeptree --all 2>/dev/null || true
