#!/usr/bin/env bash
echo "Cleaning"
mvn -B clean -pl documentation

export VERSION=""

echo "Building the doc from project root"

mvn -B javadoc:aggregate -DskipTests

echo "Cloning repo"
cd documentation || exit
mvn -B verify
mvn -B scm:check-local-modification -Dincludes=src/main/doc/antora.yml
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
antora generate target/antora/antora-playbook.yml --clean

cd target || exit
git clone -b gh-pages git@github.com:smallrye/smallrye-reactive-messaging.git site
echo "Copy content"
yes | cp -R antora/build/site/* site/
mkdir -p "site/${VERSION}"
yes | cp -R apidocs "site/${VERSION}"

echo "Pushing"
cd site  || exit
git add -A
git commit -m "update site - version ${VERSION}"
git push origin gh-pages

echo "Done"
