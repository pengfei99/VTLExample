import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.percentile_approx;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.count;


public class CompilerExample {
    private final SparkSession spark;

    public CompilerExample(SparkSession spark) {
        this.spark = spark;
    }

    public void AggregateExpressionExample() {
        // read csv file
        String path = "/home/pliu/git/VTLExample/data/users.csv";
        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv(path);
        df.show();
        df.printSchema();
        //// VTL
        // res := ds1[aggr sumAge := sum(age), avgWeight := avg(weight), countVal := count(null),
        //                  maxAge := max(age), maxWeight := max(weight), minAge := min(age),
        //                  minWeight := min(weight), medianAge := median(age), medianWeight := median(weight) group by country];


        // corresponding spark query
        //  percentile_approx("age", 0.5, 10000).alias("medianAge"),
        Dataset<Row> result = df.groupBy("country")
                .agg(sum("age").alias("sumAge"),
                        avg("weight").alias("avgWeight"),
                        max("age").alias("maxAge"),
                        min("age").alias("minAge"),
                        count("*").alias("countVal"));

        result.show();
    }

    public static void main(String[] args) {
        //prepare spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark VTL example")
                .getOrCreate();
        CompilerExample ce=new CompilerExample(spark);
        ce.AggregateExpressionExample();

    }
}
