set shell := ["bash", "-uc"]

# Just echo the purpose of this file
_default:
    @echo "This file is used to automate some release tasks"
    @echo "(running in `pwd`)"
    @just --list

# Build locally without tests
build:
    @echo "Building locally without tests"
    ./mvnw clean install -DskipTests -T1C

# Build locally without tests (fast, skipping docs, tests, revapi)
build-fast:
    @echo "Building fast locally"
    ./mvnw clean install -Dquickly -T1.5C

# Build locally without tests (fast, with local maven repository)
build-fast-local repository=".repository":
    @echo "Building fast locally with local repository"
    ./mvnw clean install -Dquickly -T1.5C -Dmaven.repo.local={{repository}}

# Build locally with tests
test:
    @echo "Testing locally"
    ./mvnw clean verify

# Build on CI without tests
build-ci:
    ./mvnw -B -ntp -s .build/ci-maven-settings.xml clean install -DskipTests

# Test on CI with tests
test-ci:
    ./mvnw -B -ntp -s .build/ci-maven-settings.xml clean verify

# Update Pulsar Connector Configuration Documentation
update-pulsar-config-docs:
    @echo "📝 Updating Pulsar connector configuration docs"
    jbang .build/PulsarConfigDoc.java -d documentation/src/main/asciidoc/_partials/pulsar/config

# Build documentation: compiles the examples and unpacks the connectors' attribute tables
build-docs:
    #!/usr/bin/env bash
    echo "📝 Building documentation"
    ./mvnw -B -ntp clean package -DskipTests -pl documentation -am

# Serve documentation the way smallrye.io shows it, and publish it again on every change
serve-docs:
    #!/usr/bin/env bash
    echo "📝 Serving documentation on http://localhost:8080/docs/smallrye-reactive-messaging/main/"
    ./mvnw -B -ntp package -DskipTests -pl documentation -am -q
    ./mvnw -Pdocs-preview

# Clear RevAPI justifications
clear-revapi:
    #!/usr/bin/env bash
    jbang .build/CompatibilityUtils.java clear
    if [[ $(git diff --stat) != '' ]]; then
      git add -A
      git status
      git commit -m "[POST-RELEASE] - Clearing breaking change justifications"
      git push
    else
      echo "No justifications cleared"
    fi
