# Releasing SmallRye Reactive Messaging

The release process starts by creating PR with a release branch.

The release branch contains a change to `.github/project.yml` setting the `current-version` to the release version.

## Review the release PR

On creation the release PR is verified for the `current-version` and whether the milestone contains any open issues or PRs.

## Prepare the release

Once the release PR is merged, the prepare release workflow is triggered.

This workflow calls the maven-release-plugin prepare goal, which sets the release version in the `pom.xml` and pushes a release tag to the repository.

It also closes the milestone associated with the release.

Then the workflow clears any RevAPI justifications and pushes the changes to the target branch (main in regular releases).

## Perform the release

The release workflow is triggered on a push of a new tag (created by the prepare step).

It deploys artifacts locally and attaches them to the action artifacts.
And publishes artifacts to the Maven Central using the Smallrye Release workflow.

When the Maven Central sync is complete, it deploys the website.

