# Pyoozie Release Instructions

To release a new version of [`pyoozie`](https://pypi.python.org/pypi/pyoozie):

1. Create a user account at [PyPI](https://pypi.python.org/pypi) and [Test PyPI](https://testpypi.python.org/pypi)
2. Ask a project maintainer (e.g. [cfournie](https://github.com/cfournie)) for access to [`pyoozie`](https://pypi.python.org/pypi/pyoozie)
3. Setup your local `~/.pypirc` with your credentials:

    ```
    [distutils]
    index-servers=
        pypi
        testpypi

    [testpypi]
    repository = https://test.pypi.org/legacy/
    username = <your user name goes here>
    password = <your password goes here>

    [pypi]
    repository = https://upload.pypi.org/legacy/
    username = <your user name goes here>
    password = <your password goes here>
    ```

4. Create a PR to increment [`__version__`](https://github.com/Shopify/pyoozie/blob/6956a38f9677eec8b6ccbfaa5280dbee0503eb20/pyoozie/__init__.py#L41) according to our [versioning standards](https://github.com/Shopify/shopify_python#versioning) that lists the features associated with this release (e.g. [this release PR](https://github.com/Shopify/pyoozie/pull/55))
    - Note that this PR does not necessarily need to be merged to `master` but should be code reviewed
    - Also note that a release does not have to be crafted from current `master` but could branch off and cherry pick specific features to release
5. Optionally run `python setup.py register -r https://test.pypi.org/legacy/` if the project has been deleted on Test PyPI
6. Run `make release` to build the release files locally
7. Run `make upload_test` to upload the release to Test PyPI
8. Test that the release works by running `pip install -i https://testpypi.python.org/pypi pyoozie` in a fresh virtualenv and make sure that:
    - It installs correctly (you may need to manually install its dependencies b/c they probably aren't on Test PyPI); and
    - You can at least import a class/function from the library
9. If everything looks OK, release to PyPI by running `make upload_pypi`
