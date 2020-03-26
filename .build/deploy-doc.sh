#!/usr/bin/env bash
echo "Cleaning"
mvn clean -pl documentation


echo "Building the doc from project root"

mvn javadoc:aggregate -DskipTests

echo "Cloning repo"
cd documentation || exit
mvn verify
mvn scm:check-local-modification -Dincludes=src/main/doc/antora.yml
export VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
antora generate target/antora/antora-playbook.yml --clean

cd target || exit
git clone -b gh-pages git@github.com:smallrye/smallrye-reactive-messaging.git site
echo "Copy content"
yes | cp -R antora/build/site/* site/2.x-preview
mkdir -p "site/2.x-preview/${VERSION}"
yes | cp -R apidocs "site/2.x-preview/${VERSION}"

echo "Pushing"
cd site  || exit
git add -A
git commit -m "update site"
git push origin gh-pages

echo "Done"
