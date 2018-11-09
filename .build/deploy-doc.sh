#!/usr/bin/env bash
echo "Cleaning"
mvn clean -N


echo "Building the doc"

mvn -N -Pdoc


echo "Cloning repo"
cd target
git clone -b gh-pages git@github.com:smallrye/smallrye-reactive-messaging.git site
echo "Copy content"
cp -R generated-docs/* site

echo "Pushing"
cd site
git add -A
git commit -m "update site"
git push origin gh-pages

echo "Done"
