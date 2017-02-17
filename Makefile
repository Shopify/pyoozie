python_files := find . -path '*/.*' -prune -o -name '*.py' -print0

all: install test lint

clean:
		find . \( -name '*.pyc' -o -name '*.pyo' -o -name '*~' \) -print -delete >/dev/null
		find . -name '__pycache__' -exec rm -rvf '{}' + >/dev/null
		rm -fr *.egg-info

install:
		pip install -e .
		pip install -r dev_requirements.txt
		test -d lib || mkdir lib
		test -f lib/oozie-client-4.1.0.jar || \
			curl http://central.maven.org/maven2/org/apache/oozie/oozie-client/4.1.0/oozie-client-4.1.0.jar -o lib/oozie-client-4.1.0.jar
		test -f lib/commons-cli-1.2.jar || \
			curl http://central.maven.org/maven2/commons-cli/commons-cli/1.2/commons-cli-1.2.jar -o lib/commons-cli-1.2.jar

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
		@flake8

autolint: autopep8 lint
