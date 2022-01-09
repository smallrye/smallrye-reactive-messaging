#!/usr/bin/env bash
set -e

VERSION=${1:-"$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"}
REMOTE=${2:-"origin"}
REMOTE_URL=$(git remote get-url "${REMOTE}")

echo "Deploying documentation version ${VERSION} to remote ${REMOTE} (${REMOTE_URL})"

echo "Configuring environment"
cd documentation
pipenv install

echo "Publishing"
mvn -B clean compile
pipenv run mike deploy --update-aliases --push --remote "${REMOTE}" "${VERSION}" "latest"
pipenv --rm

echo "Done"
