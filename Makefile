python_files := find . -path '*/.*' -prune -o -name '*.py' -print0

all: install test lint

clean:
		find . \( -name '*.pyc' -o -name '*.pyo' -o -name '*~' \) -print -delete >/dev/null
		find . -name '__pycache__' -exec rm -rvf '{}' + >/dev/null
		rm -fr *.egg-info

install:
		pip install -e .
		pip install -r dev_requirements.txt

test: clean
		py.test -vv --durations=10 --cov=pyoozie

coverage:
		py.test -vv --cov=pyoozie --cov-report html tests/

autopep8:
		@echo 'Auto Formatting...'
		@$(python_files) | xargs -0 autopep8 --max-line-length 120 --jobs 0 --in-place --aggressive

lint:
		@echo 'Linting...'
		@pylint --rcfile=pylintrc pyoozie tests

autolint: autopep8 lint
