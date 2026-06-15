# insights-ccx-messaging

[![PyPI version](https://img.shields.io/pypi/v/ccx-messaging)](https://pypi.org/project/ccx-messaging/)
[![Python versions](https://img.shields.io/pypi/pyversions/ccx-messaging)]
[![Python unit tests](https://github.com/RedHatInsights/insights-ccx-messaging/actions/workflows/pytests.yaml/badge.svg)](https://github.com/RedHatInsights/insights-ccx-messaging/actions/workflows/pytests.yaml)
[![codecov](https://codecov.io/gh/RedHatInsights/insights-ccx-messaging/branch/main/graph/badge.svg?token=G00EQ808EP)](https://codecov.io/gh/RedHatInsights/insights-ccx-messaging)
[![License](https://img.shields.io/badge/license-Apache-blue)](https://github.com/RedHatInsights/insights-ccx-messaging/blob/main/LICENSE)
[![GitHub Pages](https://img.shields.io/badge/%20-GitHub%20Pages-informational)](https://redhatinsights.github.io/insights-ccx-messaging/)

Stub for all CCX services based on Insights Core Messaging framework

<!-- vim-markdown-toc GFM -->

* [Makefile targets](#makefile-targets)
* [Releasing a new version](#releasing-a-new-version)

<!-- vim-markdown-toc -->

## Makefile targets

```
Available targets are:

unit_tests           Run unit tests
coverage             Run unit tests, display code coverage on terminal
coverage-report      Run unit tests, generate code coverage as a HTML report
documentation        Generate documentation for all sources
shellcheck           Run shellcheck
pyformat             Reformat all Python sources
help                 Show this help screen
```

## Releasing a new version

The project version is derived from git tags using [`setuptools_scm`](https://github.com/pypa/setuptools-scm) — there is no hardcoded version file.

To create a new release:

1. Tag the desired commit on `main` with a lightweight tag following the `vMAJOR.MINOR.PATCH` convention:

    ```shell
    git tag v4.X.Y
    git push origin v4.X.Y
    ```

2. The CI workflow (`.github/workflows/pypi-release.yaml`) will automatically build and publish to [PyPI](https://pypi.org/p/ccx-messaging).
