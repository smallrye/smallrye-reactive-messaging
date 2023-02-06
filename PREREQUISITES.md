## Prerequisites

The build process requires Apache Maven and Java 11+ and can be performed using:

```bash
mvn clean install
```

Some of the build uses [Testcontainers](https://www.testcontainers.org)
so you need to have a running [Docker](https://docs.docker.com/engine/install/) environment.

### Docker Troubleshooting

#### Cgroups

**Error:** docker: Error response from daemon: OCI runtime create failed: container_linux.go:345: starting container process

This might be because your OS is using cgroupsV2 by default, which is not yet supported by the container runtimes.

**Fix:** Disable cgroupsV2:

* Fedora: `sudo grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"` then restart your machine.
* Ubuntu: `sudo update-grub "systemd.unified_cgroup_hierarchy=0""` then restart your machine.
