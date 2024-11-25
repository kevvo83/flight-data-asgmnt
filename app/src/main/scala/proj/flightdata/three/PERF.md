### Repartition, order by and group by

Op
```scala
val res = flightDataDf.
    repartition(col("passengerId")).
    orderBy(asc("date")).
    groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations"))
```

Physical plan
```
== Physical Plan ==
ObjectHashAggregate(keys=[passengerId#0], functions=[collect_list(from#2, 0, 0)])
+- Exchange hashpartitioning(passengerId#0, 200)
   +- ObjectHashAggregate(keys=[passengerId#0], functions=[partial_collect_list(from#2, 0, 0)])
      +- *(2) Project [passengerId#0, from#2]
         +- *(2) Sort [date#4 ASC NULLS FIRST], true, 0
            +- Exchange rangepartitioning(date#4 ASC NULLS FIRST, 200)
               +- Exchange hashpartitioning(passengerId#0, 200)
                  +- *(1) FileScan csv [passengerId#0,from#2,date#4] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/kevinlawrence/IdeaProjects/flight-data-asgmnt/app/src/main/resource..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<passengerId:int,from:string,date:date>
```

### Partition and save as table, then group by
Op 
```scala
// first partition the data in a table
flightDataDf.
    write.
    mode(SaveMode.Overwrite).
    partitionBy("passengerId").saveAsTable("flight_data_part_by_pass_id")

  val table_def = spark.table(table_name)

  table_def.printSchema()
  table_def.show(20)
  table_def.explain()

// Then operate on the partitioned table
  val res = spark.
    table(table_name).
    sortWithinPartitions(asc("date")).
    groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations")).
    withColumn("spans", getRunsAvoidingUk(col("locations")))

  res.printSchema()
  res.show(20)
  res.explain()
```