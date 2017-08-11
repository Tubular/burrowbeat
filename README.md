# Burrowbeat

Welcome to Burrowbeat.

Ensure that this folder is at the following location:
`${GOPATH}/github.com/goomzee/burrowbeat`

## Getting Started with Burrowbeat

### Requirements

* [Golang](https://golang.org/dl/) 1.7

### Init Project
To get running with Burrowbeat and also install the
dependencies, run the following command:

```
make setup
```

It will create a clean git history for each major step. Note that you can always rewrite the history if you wish before pushing your changes.

To push Burrowbeat in the git repository, run the following commands:

```
git remote set-url origin https://github.com/goomzee/burrowbeat
git push origin master
```

For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).

### Build

To build the binary for Burrowbeat run the command below. This will generate a binary
in the same directory with the name burrowbeat.

```
make
```


### Run

To run Burrowbeat with debugging output enabled, run:

```
./burrowbeat -c burrowbeat.yml -e -d "*"
```

### Exported fields

There are two types of documents exported:
- `type: consumer_group` for consumer group partitions and lag
- `type: topic` for topic name, size, partitions and lag

Consumer group:

<pre>
{
    "beat": {
        "name": "burrowbeat",
        "host": "localhost"
    },
    "@timestamp": "YYYY-MM-DD:MM:SS.milliZ",
    "@version": "1",
    "type": "consumer_group",
    "count": 1,
    "cluster": "cluster_name",
    "consumer_group": {
         "name": "consumer_group_name",
         "offset": 0,
         "lag": 0,
         "partition": 11,
         "topic": "topic_name"
    }
}
</pre>

Topic:

<pre>
{
    "beat": {
        "name": "burrowbeat",
        "host": "localhost"
    },
    "@timestamp": "YYYY-MM-DD:MM:SS.milliZ",
    "@version": "1",
    "type": "topic",
    "count": 1,
    "cluster": "cluster_name",
    "group": "consumer_group_name",
    "topic": {
        "name": "test",
        "size": 0,
        "partitions": 2,
        "lag": 0
     }
}
-</pre>

### Test

To test Burrowbeat, run the following command:

```
make testsuite
```

alternatively:
```
make unit-tests
make system-tests
make integration-tests
make coverage-report
```

The test coverage is reported in the folder `./build/coverage/`

### Update

Each beat has a template for the mapping in elasticsearch and a documentation for the fields
which is automatically generated based on `etc/fields.yml`.
To generate etc/burrowbeat.template.json and etc/burrowbeat.asciidoc

```
make update
```


### Cleanup

To clean  Burrowbeat source code, run the following commands:

```
make fmt
make simplify
```

To clean up the build directory and generated artifacts, run:

```
make clean
```


### Clone

To clone Burrowbeat from the git repository, run the following commands:

```
mkdir -p ${GOPATH}/github.com/goomzee
cd ${GOPATH}/github.com/goomzee
git clone https://github.com/goomzee/burrowbeat
```


For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).


## Packaging

The beat frameworks provides tools to crosscompile and package your beat for different platforms. This requires [docker](https://www.docker.com/) and vendoring as described above. To build packages of your beat, run the following command:

```
make package
```

This will fetch and create all images required for the build process. The hole process to finish can take several minutes.
