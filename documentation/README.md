# Documentation

This module contains the SmallRye Reactive Messaging documentation.
The documentation uses:

- mkdocs
- markdown

## Prerequisites

* python 3
* pipenv

pipenv can be installed via pip:
```shell
pip3 install pipenv
```

## Development

Create virtualenv and install requirements:
```shell
> pipenv shell
> pipenv install
```

Utilities needed for development such as `mkdocs` and `mike` will be available inside created shell.

When finished, you can remove the virtual environment and exit the created sub-shell:
```shell
> pipenv --rm
> exit
```

## Build

```shell
> mvn compile
> mkdocs serve # Live view on http://127.0.0.1:8000/smallrye-reactive-messaging/

# Or build
> mkdocs build
```

## Deploy

```shell
mike deploy $VERSION -p
mike alias $VERSION "latest"
```

## Structure

### The navigation

The navigation is described in the `mkdocs.yml` file.

### The content

The documentation sources are in `src/main/docs`.

### Attributes

We extended mkdocs with a set of macros (named `docissimo`) which loads versions from Maven.
The source of the loaded file in `src/main/resources/attributes.yaml`.
It contains *filtered* variables, and the output (loaded by the macro) is in `target/classes/attributes.yaml`.

Access the values as follows:

```text
{{ attributes['project-version'] }}
```

### Snippets

Code snippets are located in `src/main/java`.
They are inserted using a docissimo macros:

```text
{{ insert('rabbitmq/customization/RabbitMQProducers.java', 'named') }}
```

The section to insert (like `named` in the previous example) is delimited with `<named></named>`.
If you don't set the section, the whole file is included.

### Connector tables

The connector configuration tables are unpacked from each connector in `target/connectors`, and so inserted as follows:

```text
{{ insert('../../../target/connectors/smallrye-rabbitmq-incoming.md') }}
```

NOTE: The insertion *root* is `src/main/java`.
