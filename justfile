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
    jbang .build/PulsarConfigDoc.java -d documentation/src/main/docs/pulsar/config

# Build documentation
build-docs:
    #!/usr/bin/env bash
    echo "📝 Building documentation"
    ./mvnw -B -ntp clean compile -pl documentation
    cd documentation
    pipenv install
    pipenv run mkdocs build

# Serve documentation
serve-docs:
    #!/usr/bin/env bash
    echo "📝 Building documentation"
    ./mvnw -B -ntp clean compile -pl documentation
    cd documentation
    pipenv install
    pipenv run mkdocs serve

# Deploy documentation
deploy-docs version:
    #!/usr/bin/env bash
    echo "📝 Deploying documentation to GitHub"
    ./mvnw -B -ntp clean compile -pl documentation
    cd documentation
    pipenv install
    pipenv run mike deploy --update-aliases --push --remote origin {{version}} $(git merge-base --is-ancestor HEAD origin/main && echo '"latest"' || echo '')

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
