# Py Oozie

[![Build Status](https://travis-ci.org/Shopify/pyoozie.svg?branch=master)](https://travis-ci.org/Shopify/pyoozie)
[![PyPI](https://img.shields.io/pypi/v/nine.svg)](https://pypi.python.org/pypi/pyoozie)
[![PyPI](https://img.shields.io/pypi/pyversions/pyoozie.svg)](https://pypi.python.org/pypi/pyoozie)

This is a library for querying and scheduling with [Apache Oozie](https://oozie.apache.org/).

## Supported versions

- Oozie 4.1.0
- Python 2.7 and 3.4+

## Contributing

Let's build a codebase that we can be proud of, that enables us to deliver better results, faster and more confidently.

We can start with the following guidelines:

- Make sure your code is readable.
- Don't hesitate to refactor code where it improves the design.
- Don't take shortcuts; development is more like a marathon than a sprint.
- And leave the code cleaner than you found it.

This project follows the [Google Python Style Guide](http://google.github.io/styleguide/pyguide.html) with the following
exceptions:
- Line Length: The maximum line length is 120 columns.

## Developing
This project also targets both Python 2.7 and Python 3.3+; it's recommended to setup virtual environments for both
Python 2 and 3 when developing, e.g.:

```
mkvirtualenv -a $(pwd) pyooziepy2 -p python2
mkvirtualenv -a $(pwd) pyooziepy3 -p python3
```

## Testing

All committed code requires quality tests that provide ample code coverage. Functionality that is not covered by tests
should be assumed to be broken (and probably will be).

## Code Review

Committing to `master` requires a code review, submitted in the form of a GitHub Pull Request. To be merged, a PR must
have an explicit approval from a core team member and no outstanding questions.