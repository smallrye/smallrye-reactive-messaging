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

compatibility_extract() {
  echo "Extracting compatibility report"
  jbang .github/CompatibilityUtils.java extract
}

compatibility_clear() {
  echo "Clearing difference justifications"
  jbang .github/CompatibilityUtils.java clear
  if [[ $(git diff --stat) != '' ]]; then
    git add -A
    git status
    git commit -m "[POST-RELEASE] - Clearing breaking change justifications"
    git push origin main
  else
    echo "No justifications cleared"
  fi
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
      echo "Setting main version to ${NEXT_VERSION}-SNAPSHOT"
      git reset --hard
      git checkout main
      mvn -B versions:set -DnewVersion="${NEXT_VERSION}-SNAPSHOT" -DgenerateBackupPoms=false -s maven-settings.xml
      git commit -am "[RELEASE] - Bump main branch to ${NEXT_VERSION}-SNAPSHOT"
      git push origin main
      echo "Main branch updated"
    else
        echo "No next version - skip updating the main version"
    fi
}

init_git
init_gpg

if [[ ${TARGET} == "release" ]]; then
    echo "Checking release prerequisites"
    echo "Milestone set to ${MILESTONE}"
    .github/Prelease.java --token="${GITHUB_TOKEN}" --release-version="${MILESTONE}"

    deploy_release

    echo "Executing post-release"
    compatibility_extract
    .github/PostRelease.java --token="${GITHUB_TOKEN}"
    compatibility_clear
else
    echo "Unknown environment: ${TARGET}"
fi
