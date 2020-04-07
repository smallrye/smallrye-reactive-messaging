#!/usr/bin/env kscript
@file:MavenRepository("jcenter","https://jcenter.bintray.com/")
@file:MavenRepository("maven-central","https://repo1.maven.org/maven2/")
@file:DependsOn("org.kohsuke:github-api:1.101")

/**
 * Script checking that the repository can be released.
 * It checks that:
 * - the milestone has been set, and exists
 * - the milestone has no opened issues
 * - the tag does not exist
 *
 * Run with `./pre-release.kts ${GIHUB_TOKEN} ${MILESTONE}
 *
 * 1. The github token is mandatory.
 * 2. The targeted milestone.
 *
 * The milestone (mandatory) is retrieved from the `MILESTONE` env variable.
 */

import org.kohsuke.github.*
import java.io.File

val REPO = "smallrye/smallrye-reactive-messaging"

val token = args[0]; // Fail if the first argument is not there
val milestone_name = args[1]; // Fail if the second argument is not there

val github = GitHubBuilder().withOAuthToken(token).build()
val repository = github.getRepository(REPO)


val newVersion = milestone_name;
println("New version would be ${newVersion}")

println("Checking the existence of the ${newVersion} milestone")
val milestones = repository.listMilestones(GHIssueState.ALL).asList()
failIfPredicate({ milestones.isNullOrEmpty() }, "No milestones in repository ${repository.getName()}")
val existingMilestone = milestones.find { it.getTitle() == newVersion}
failIfPredicate( { existingMilestone == null }, "There is no milestone with name ${newVersion}, invalid increment")
println("Checking the number of open issues in the milestone ${newVersion}")
failIfPredicate( { existingMilestone?.getOpenIssues() != 0 }, "The milestone ${newVersion} has ${existingMilestone?.getOpenIssues()} issues opened, check ${existingMilestone?.getHtmlUrl()}")

println("Listing tags of ${repository.getName()}")
val tags = repository.listTags().asList()
failIfPredicate({ tags.isNullOrEmpty() }, "No tags in repository ${repository.getName()}")
val tag = tags.first()
println("Last tag is " + tag.getName())

println("Checking the absence of the ${newVersion} tag")
val existingTag = tags.find { it.getName() == newVersion}
failIfPredicate( { existingTag != null }, "There is a tag with name ${newVersion}, invalid new release version")

println("Write computed version to /tmp/release-version")
File("/tmp/release-version").writeText(newVersion)

fun fail(message: String, code: Int = 2) {
    println(message)
    kotlin.system.exitProcess(code)
}

fun failIfPredicate(predicate: () -> Boolean, message: String, code: Int = 2) {
    val success = predicate.invoke()
    if (success) {
        fail(message, code)
    }
}



