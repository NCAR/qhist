build:
	python3 -m build

upload:
	python3 -m twine upload --repository testpypi dist/*

clean:
	rm -rf dist build
