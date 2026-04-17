## Building from source

To build the project from source, follow these steps:

1. Clone the repository
2. `make build` (to build the project binaries `lakectl` and `lakefs`)
3. `make build-docker` (optional, requires Docker)

## Bump version

To bump the version of the project:

1. Update the `VERSION` variable in `Makefile`
2. Update `clients/python-wrapper/setup.py` with the new version
