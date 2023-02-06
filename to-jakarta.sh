#!/usr/bin/env bash

# update project major version
mvn build-helper:parse-version versions:set -DnewVersion=\${parsedVersion.nextMajorVersion}.0.0-SNAPSHOT

# move to jakarta parent
find . -type f -name 'pom.xml' -exec sed -i '' 's/smallrye-parent/smallrye-jakarta-parent/g' {} +

# move versions that support jakarta
mvn versions:set-property -Dproperty=weld.version -DnewVersion=5.1.0.Final
mvn versions:set-property -Dproperty=microprofile-reactive-messaging.version -DnewVersion=3.0
mvn versions:set-property -Dproperty=microprofile-reactive-streams.version -DnewVersion=3.0
mvn versions:set-property -Dproperty=microprofile-config.version -DnewVersion=3.0.1
mvn versions:set-property -Dproperty=microprofile-metrics-api.version -DnewVersion=4.0.1
mvn versions:set-property -Dproperty=microprofile-health-api.version -DnewVersion=4.0
mvn versions:set-property -Dproperty=smallrye-config.version -DnewVersion=3.0.0
mvn versions:set-property -Dproperty=smallrye-metrics.version -DnewVersion=4.0.0
mvn versions:set-property -Dproperty=smallrye-common.version -DnewVersion=2.0.0
mvn versions:set-property -Dproperty=smallrye-health.version -DnewVersion=4.0.0
mvn versions:set-property -Dproperty=smallrye-testing.version -DnewVersion=2.2.0
mvn versions:set-property -Dproperty=smallrye-fault-tolerance.version -DnewVersion=6.1.0
mvn versions:set-property -Dproperty=yasson.version -DnewVersion=2.0.3
mvn versions:set-property -Dproperty=artemis.version -DnewVersion=2.20.0

# artemis jakarta
find . -type f -name 'pom.xml' -exec sed -i '' 's/\<artifactId\>artemis-jms-client\<\/artifactId\>/\<artifactId\>artemis-jakarta-client\<\/artifactId\>/g' {} +
find . -type f -name 'pom.xml' -exec sed -i '' 's/\<artifactId\>artemis-server\<\/artifactId\>/\<artifactId\>artemis-jakarta-server\<\/artifactId\>/g' {} +

# camel not ready yet
find . -type f -name 'pom.xml' -exec sed -i '' 's/\<module\>smallrye-reactive-messaging-camel\<\/module\>/\<\!--\<module\>smallrye-reactive-messaging-camel\<\/module\>-->/g' {} +

# java sources
find . -type f -name '*.java' -exec sed -i '' 's/javax./jakarta./g' {} +
find . -type f -name '*.kt' -exec sed -i '' 's/javax./jakarta./g' {} +
find . -type f -name '*.java' -exec sed -i '' 's/jakarta.lang./javax.lang./g' {} +
find . -type f -name '*.java' -exec sed -i '' 's/jakarta.tools./javax.tools./g' {} +
find . -type f -name '*.java' -exec sed -i '' 's/jakarta.annotation.processing./javax.annotation.processing./g' {} +
find . -type f -name '*.java' -exec sed -i '' 's/jakarta.net./javax.net./g' {} +
# service loader files
find . -path "*/src/main/resources/META-INF/services/javax*" | sed -e 'p;s/javax/jakarta/g' | xargs -n2 git mv
# docs
find . -type f -name '*.adoc' -exec sed -i '' 's/javax./jakarta./g' {} +
find . -type f -name '*.adoc' -exec sed -i '' 's/jakarta.lang./javax.lang./g' {} +
find . -type f -name '*.adoc' -exec sed -i '' 's/jakarta.tools./javax.tools./g' {} +
find . -type f -name '*.adoc' -exec sed -i '' 's/jakarta.annotation.processing./javax.annotation.processing./g' {} +
find . -type f -name '*.md' -exec sed -i '' 's/javax./jakarta./g' {} +
find . -type f -name '*.md' -exec sed -i '' 's/jakarta.lang./javax.lang./g' {} +
find . -type f -name '*.md' -exec sed -i '' 's/jakarta.tools./javax.tools./g' {} +
find . -type f -name '*.md' -exec sed -i '' 's/jakarta.annotation.processing./javax.annotation.processing./g' {} +

# commit all changes
git add -A
git commit -m "Update to Jakarta API"
# apply patches
git am jakarta/*.diff
