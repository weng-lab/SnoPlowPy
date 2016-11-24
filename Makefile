.PHONY: clean clean-build clean-pyc clean-test lint test test-doc docs

help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "test-doc - run Sphinx documentation integrity check"
	@echo "docs - generate Sphinx HTML documentation, including API docs"

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage

lint:
	flake8 snoPlowPy

test: clean-pyc
	pytest snoPlowPy

test-doc:
	sphinx-build -b html -d docs/_build/doctrees docs docs/_build/html

docs:
	rm -f docs/snoPlowPy.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ snoPlowPy
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
