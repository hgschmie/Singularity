## Installing Singularity

*If you just want to test out Singularity, consider using our [vagrant](vagrant.md) box instead.*

### 1. Set up a Zookeeper cluster

Singularity uses Zookeeper as its primary datastore -- it cannot run without it.

Chef recipe: https://supermarket.chef.io/cookbooks/zookeeper
Puppet module: https://forge.puppetlabs.com/deric/zookeeper

More info on how to manually set up a Zookeeper cluster lives here: https://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_zkMulitServerSetup

For testing or local development purposes, a single-node cluster running on your local machine is fine.

### 2. Set up MySQL (optional)

Singularity can be configured to move stale data from Zookeeper to MySQL after a configurable amount of time, which helps reduce strain on the cluster. If you're running Singularity in Production environment, MySQL is encouraged.

### 3. Set up a Mesos cluster

Mesosphere provides a good tutorial for setting up a Mesos cluster: http://mesosphere.com/docs/getting-started/datacenter/install/. Don't bother setting up Marathon, it isn't necessary for Singularity.

### 4. Build or download the Singularity JAR

In order to run Singularity, you can either build it from scratch or download a precompiled JAR.

#### Building from Source

Run `mvn clean package` in the root of the Singularity repository. The SingularityService JAR will be created in `SingularityService/target/`.

#### Downloading a precompiled JAR

Singularity JARs are published to Maven Central for each release. You can view the list of SingularityService (the executable piece of Singularity) here: http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.hubspot%22%20AND%20a%3A%22SingularityService%22

Be sure to only use the `shaded.jar` links -- the other JARs won't work.

### 5. Create Singularity config file

Singularity requires a YAML file with some configuration values in order to start up. Here's an example:

```yaml
server:
  type: simple
  applicationContextPath: /singularity
  connector:
    type: http
    port: 7099
  requestLog:
    appenders:
      - type: file
        currentLogFilename: ../logs/access.log
        archivedLogFilenamePattern: ../logs/access-%d.log.gz

database:  # omit this entirely if not using MySQL
  driverClass: com.mysql.jdbc.Driver
  user: [database username]
  password: [database password]
  url: jdbc:mysql://[database host]:[database port]/[database name]

mesos:
  master: zk://[comma separated host:port list of ZK hosts]/mesos
  defaultCpus: 1
  defaultMemory: 128
  frameworkName: Singularity
  frameworkId: Singularity
  frameworkFailoverTimeout: 1000000

zookeeper:
  quorum: [comma separated host:port list of ZK hosts]
  zkNamespace: singularity
  sessionTimeoutMillis: 60000
  connectTimeoutMillis: 5000
  retryBaseSleepTimeMilliseconds: 1000
  retryMaxTries: 3

logging:
  loggers:
    "com.hubspot.singularity" : TRACE

enableCorsFilter: true
sandboxDefaultsToTaskId: false  # enable if using SingularityExecutor

ui:
  title: Singularity (local)
  baseUrl: http://localhost:7099/singularity
```

Full configuration documentation lives here: [configuration.md](reference/configuration.md)

### 6. Run MySQL migration (if necessary)

If you're operating Singularity with MySQL, you first need to run a liquibase migration to create all appropriate tables: (this snippet assumes your Singularity configuration YAML exists as `singularity_config.yaml`)

`java SingularityService/target/SingularityService-*-shaded.jar db migrate singularity_config.yaml --migrations mysql/migrations.sql`

It's a good idea to run a migration each time you upgrade to a new version of Singularity.

### 7. Start Singularity

`java -jar SingularityService/target/SingularityService-*-shaded.jar server singularity_config.yaml`

**Warning:** Singularity will abort (i.e. exit) when it hits an unrecoverable error, so it's a good idea to use monit, supervisor, or systemd to monitor and restart the Singularity process. It does this in order to simplify the handling of state which must be kept in sync with the Mesos master.

### 8. Install extra Singularity tools on Mesos slaves (optional)

Singularity ships with a custom Mesos executor and extra background jobs to make running tasks easier. More info lives in [slave_extras.md](slave_extras.md).
