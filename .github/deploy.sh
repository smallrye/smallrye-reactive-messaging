#!/usr/bin/env bash
set -e

export TARGET=$1

init_gpg() {
    gpg2 --fast-import --no-tty --batch --yes smallrye-sign.asc
}

init_git() {
    git config --global user.name "${GITHUB_ACTOR}"
    git config --global user.email "smallrye@googlegroups.com"

    git update-index --assume-unchanged .github/deploy.sh
    git update-index --assume-unchanged .github/decrypt-secrets.sh
    git update-index --assume-unchanged .github/deploy-doc.sh
}

deploy_release() {
    export RELEASE_VERSION=""
    export BRANCH="HEAD"
    export NEXT_VERSION=""

    if [ -f /tmp/release-version ]; then
      RELEASE_VERSION=$(cat /tmp/release-version)
    else
        echo "'/tmp/release-version' required"
        exit 1
    fi

    echo "Cutting release ${RELEASE_VERSION}"
    mvn -B -fn clean
    git checkout ${BRANCH}
    HASH=$(git rev-parse --verify $BRANCH)
    echo "Last commit is ${HASH} - creating detached branch"
    git checkout -b "r${RELEASE_VERSION}" "${HASH}"

    echo "Update version to ${RELEASE_VERSION}"
    mvn -B versions:set -DnewVersion="${RELEASE_VERSION}" -DgenerateBackupPoms=false -s maven-settings.xml
    mvn -B clean verify -DskipTests -Prelease -s maven-settings.xml

    git commit -am "[RELEASE] - Bump version to ${RELEASE_VERSION}"
    git tag "${RELEASE_VERSION}"
    echo "Pushing tag to origin"
    git push origin "${RELEASE_VERSION}"

    echo "Deploying release artifacts"
    mvn -B deploy -DskipTests -Prelease -s maven-settings.xml

    echo "Building documentation"
    .github/deploy-doc.sh

    if [ -f /tmp/next-version ]; then
      NEXT_VERSION=$(cat /tmp/next-version)
      echo "Setting master version to ${NEXT_VERSION}-SNAPSHOT"
      git reset --hard
      git checkout master
      mvn -B versions:set -DnewVersion="${NEXT_VERSION}-SNAPSHOT" -DgenerateBackupPoms=false -s maven-settings.xml
      git commit -am "[RELEASE] - Bump master to ${NEXT_VERSION}-SNAPSHOT"
      git push origin master
      echo "Master updated"
    else
        echo "No next version - skip updating the master version"
    fi
}

init_git
init_gpg

if [[ ${TARGET} == "release" ]]; then
    echo "Checking release prerequisites"
    echo "Milestone set to ${MILESTONE}"
    .github/pre-release.kts "${GITHUB_TOKEN}" "${MILESTONE}"

    deploy_release

    echo "Executing post-release"
    .github/post-release.kts "${GITHUB_TOKEN}"
else
    echo "Unknown environment: ${TARGET}"
fi
