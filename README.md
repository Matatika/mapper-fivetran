# mapper-fivetran

`mapper-fivetran` is a Singer mapper for Fivetran.

Built with the [Meltano Mapper SDK](https://sdk.meltano.com) for Singer Mappers.

## Overview

`mapper-fivetran` maps incoming data to a Fivetran-compatible format. This involves:

- Flattening top-level properties
- Converting property names to snake-case
- Adding properties pertaining to [Fivetran system columns](https://fivetran.com/docs/core-concepts/system-columns-and-tables)
  - `_fivetran_id`: MD5-hash of a record, added when no `key_properties` are defined for the stream
  - `_fivetran_synced`: ISO8601 timestamp of when the record was initally extracted, or otherwise processed by the mapper
  - `_fivetran_deleted`: boolean to indicate soft-delete 

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPI repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPI:

```bash
pipx install mapper-fivetran
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/mapper-fivetran.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the mapper.

This section can be created by copy-pasting the CLI output from:

```
mapper-fivetran --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
mapper is available by running:

```bash
mapper-fivetran --about
```

### Configure using environment variables

This Singer mapper will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your mapper requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `mapper-fivetran` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Mapper Directly

```bash
mapper-fivetran --version
mapper-fivetran --help
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `mapper-fivetran` CLI interface directly using `uv run`:

```bash
uv run mapper-fivetran --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This mapper will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd mapper-fivetran
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Run a test `run` pipeline:
meltano run tap-smoke-test mapper-fivetran target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps, targets, and mappers.
