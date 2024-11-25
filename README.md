# flight-data-asgmnt

## Instructions

### Build project

```commandline
./gradlew build
```

### Run application to generate responses

#### Generate answers for questions 1 and 2

```cmd
./gradlew run -PchooseMain=proj.flightdata.oneandtwo.App
```

#### Generate answers for question 3

```cmd
./gradlew run -PchooseMain=proj.flightdata.three.App
```

#### Generate answers for question 4

```commandline
./gradlew run -PchooseMain=proj.flightdata.four.App
```

#### Tests

```commandline
./gradlew :app:test --tests "proj.flightdata.four.DataTest"
./gradlew :app:test --tests "proj.flightdata.three.TestUDF"
```

### Debug using the Scala REPL

```cmd
cd ~
scala -cp ".gradle/caches/modules-2/files-2.1" 
```

```cmd
### Start Spark History Server on the mac
```shell
/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/sbin/start-history-server.sh --properties-file log.properties
```
```
