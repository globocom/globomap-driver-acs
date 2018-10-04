# Makefile for globomap-driver-acs

# Version package
VERSION=$(shell python -c 'import globomap_driver_acs; print(globomap_driver_acs.__version__)')

# Pip executable path
PIP := $(shell which pip)

help:
	@echo
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  clean      to clean garbage left by builds and installation"
	@echo "  compile    to compile .py files (just to check for syntax errors)"
	@echo "  test       to execute all tests"
	@echo "  setup      to setup environment locally to run project"
	@echo "  install    to install"
	@echo "  dist       to create egg for distribution"
	@echo "  publish    to publish the package to PyPI"
	@echo

clean:
	@echo "Cleaning project ..."
	@rm -rf build dist *.egg-info
	@find . \( -name '*.pyc' -o -name '**/*.pyc' -o -name '*~' \) -delete

compile: clean
	@echo "Compiling source code..."
	@python -tt -m compileall .
	@pycodestyle --format=pylint --statistics globomap_driver_acs setup.py

tests: clean ## Make tests
	@nosetests --verbose --rednose  --nocapture --cover-package=globomap_driver_acs --with-coverage; coverage report -m

tests_ci: clean ## Make tests to CI
	@nosetests --verbose --rednose  --nocapture --cover-package=globomap_driver_acs


setup: requirements_test.txt
	$(PIP) install -r $^

install:
	@python setup.py install

dist: clean
	@python setup.py sdist

publish: clean dist
	@echo 'Ready to release version ${VERSION}? (ctrl+c to abort)' && read
	twine upload dist/*
	@git tag ${VERSION}
	@git push --tags
