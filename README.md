# PHPETL - Postgres Target

An implementation of a [Singer](https://www.singer.io/) data target for Postgres.

## Installation

    composer require phpetl/target-postgres

## Configuration

This target requires the use of a configuration file to talk to the Postgres database.

Copy the `config.json.dist` file to `config.json` and edit with the appropriate values for your database.

## Usage

As with other Singer-compatible targets, this target expects the data to ingest to be piped to it from a compatible tap. This can either be a file in the correct [tap format](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output) or by piping the output from a tap directly into this target.

    cat data.json | bin/target-postgres --config /path/to/config.js

The target will attempt to create a compatible Postgres table based on the schemas provided. The target will not alter an existing table, so if your schema changes over time you will need to drop the table and let it re-create. This is overall better for data integrity anyway, as new columns means already-ingested data may not get properly extracted from the source system.