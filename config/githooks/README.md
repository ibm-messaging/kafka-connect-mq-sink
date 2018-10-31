To use these, copy them into `.git/hooks`


### `commit-msg`
This will run the `gitlint` linter on your commit messages while you write them, so you can check them locally before pushing them for Travis to check.

#### Pre-reqs:
- Python
- `pip install gitlint`

#### Install:
- `cp config/githooks/commit-msg .git/hooks/.`

#### Uninstall:
- `rm .git/hooks/commit-msg`
