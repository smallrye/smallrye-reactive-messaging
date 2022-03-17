# Move to Jakarta

## With Script

- Run `to-jakarta.sh`. This will handle all migrations from `javax` to `jakarta`.

## With OpenRewrite

[OpenRewrite](https://github.com/openrewrite/rewrite) is a Semantic code search and transformation.

- Run `mvn:rewrite:run` from the root folder
- Recipes are in `rewrite.yml`

### Limitations

- Requires [OpenRewrite] version `4.21.0` (not released yet), `SNAPSHOT` is used for now
  - May require local build of https://github.com/openrewrite/rewrite
  - May require local build of https://github.com/openrewrite/rewrite-maven-plugin
- No Recipe to update Maven Project version
  - Use Maven `versions` plugins
  - Not required if using a `999-SNAPSHOT`
- No Recipe to update documentation
- Current migration Recipes do not change String descriptions (for instance error messages)

### Multi-Module

- Recipes can be set per module.
- Requires the plugin definition and configuration on the module
- Recipes need to be available in the root `rewrite.yml` (or in the classpath)
