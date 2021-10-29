# Development Log


### Onboarding

- Forked from [Upstream Repository](https://github.com/smallrye/smallrye-reactive-messaging/issues/1158) to [This Repository](https://github.com/LucienBrule/smallrye-reactive-messaging)
- Cloned repo
- Open repo in intellij
- Installed Dependancies
  - mvn install
  - mvn test
    - amqp contribution had 8 errors, had to install skipping tests
    - everything else passesd

- Made feature branch ```pr-nats-1158```
- Attempted to build documentation
```shell
(base) ❯ documentation (pr-nats-1158) ✔ antora generate target/antora/antora-playbook.yml --clean
error: configuration param 'content.edit_url' not declared in the schema
Add the --stacktrace option to see the cause.
```
- Tried to fix content.edit_url by changing it from ~ to '~'
  - Didn't work so I gave up ```¯\_(ツ)_/¯```
  - Read through adoc documentation instead
- Modified README.md
- Added ```smallrye-reactive-messaging-nats``` module
  - Right Click Project > New > Module > Maven
  - adds this as child module to parent module
- Added project contribution tracking information
- Modified devlog

**Thoughts:**
- expected the tests to pass on clean main branch
- read through ci, looks like it builds in ci but not locally so idk

### Pre implementation

- copied questions from issue to notes.md
- research and answer notes

