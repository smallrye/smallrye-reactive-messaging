#!/usr/bin/env bash
echo "Cleaning"
mvn clean -N


echo "Building the doc from project root"

mvn javadoc:aggregate -DskipTests

echo "Cloning repo"
cd documentation || exit
mvn verify

# TODO Check that src/main/doc/antora.yml is not dirty
antora generate target/antora/antora-playbook.yml --clean

cd target || exit
git clone -b gh-pages git@github.com:smallrye/smallrye-reactive-messaging.git site
echo "Copy content"
cp -R antora/build/site/* site/2.x-preview
# TODO Versioning of the javadoc
cp -R apidocs site/2.x-preview

echo "Pushing"
cd site  || exit
git add -A
git commit -m "update site"
git push origin gh-pages

echo "Done"
