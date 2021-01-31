JDK 1.8  
Scala 2.11.12

```shell
mvn clean install
```

```shell
spark-submit --class com.luisrus.SparkContextReadTxtFile --jars D:\.m2\repos\com\typesafe\config\1.4.0\config-1.4.0.jar C:\Users\Luis\IdeaProjects\SparkTest\SparkTest\target\SparkTestApp-1.0-SNAPSHOT.jar
```

```shell
spark-submit --class com.luisrus.ReadCSV C:\Users\Luis\IdeaProjects\SparkTest\SparkTest\target\SparkTestApp-1.0-SNAPSHOT.jar C:\Users\Luis\IdeaProjects\SparkTest\SparkTest\src\main\resources\drivers.csv > test.txt
```
