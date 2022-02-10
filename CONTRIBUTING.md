# Contributing

## General code rules

For docstrings, we're following [Google's documentation conventions](https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html#example-google).

All methods meant to be used by end-user **must be documented**; **no exceptions!**

## Submitting a pull request

*Note: Every PR requires at least two reviews from at least two of the core review members. It won't be possible to merge changes without at least two approvals. List of members are available on [Settings/Members page](https://gitlab.cee.redhat.com/ccx/insights-ocp/-/project_members).*

Before you submit your pull request, please consider the following guidelines:

1) Fork the repository and clone your fork:
    1) Open the following URL in your browser: <https://github.com/RedHatInsights/insights-ccx-messaging/>.
    1) Click on the "Fork" button (near the top-right corner).
    1) Open your forked repository in browser: `https://github/YOUR-USERNAME/insights-ccx-messaging/`
    1) Click on the green "Clone or download" button and copy the clone URL of your fork of the repository.
    1) Clone the repository to get a local copy that you can edit:

        ```shell
        git clone REPOSITORY-URL
        ```

1) Make your changes in a new git branch:

    ```shell
    git checkout -b bug/my-fix-branch master
    ```

1) Create your patch, **ideally including appropriate test cases**.

1) Make sure all your changes to Python code follow the `pycodestyle` guidelines. Aside from that, [EditorConfig](https://editorconfig.org/) rules must be satisfied, not just for Python files, but also for all configuration files. You can check it using [this Go utility](https://github.com/editorconfig-checker/editorconfig-checker) or let your editor handle it for you (see the [official documentation](https://editorconfig.org/#download) for more details).

1) Include documentation that either describe a change to a behavior or the changed capability to an end user.

1) Commit your changes using **a descriptive commit message**. If you are fixing an issue, please include something like "closes #xyz" or "fixes #xyz", where `xyz` is the number of the issue.

1) Push your branch to GitHub:

    ```shell
    git push origin bug/my-fix-branch
    ```

1) When opening a pull request, select the `master` branch as a base.

1) Mark your pull request with **[WIP]** (Work In Progress) to get feedback but prevent merging (e.g. [WIP] Update CONTRIBUTING.md).

1) If we suggest changes, then please:
    1) Make the required modifications.
    1) Push the changes to GitHub (this will automatically update your pull request) by either:
        - Adding a new commit and pushing:

            ```shell
            git commit
            git push origin bug/my-fix-branch
            ```

        - Rebasing your branch and force-pushing:

            ```shell
            git rebase -i master
            git push -f origin bug/my-fix-branch
            ```

That's it! Thank you for your contribution!
