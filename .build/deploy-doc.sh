#!/usr/bin/env bash
echo "Cleaning"
mvn clean -N


echo "Building the doc"

mvn -N -Pdoc


echo "Cloning repo"
cd target
git clone -b gh-pages git@github.com:smallrye/smallrye-reactive-messaging.git site
echo "Copy content"
cp -R generated-doc site

echo "Pushing"
cd site
git push origin gh-pages

echo "Done"
