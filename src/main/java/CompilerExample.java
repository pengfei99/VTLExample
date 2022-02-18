import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.percentile_approx;


public class CompilerExample {
    private final SparkSession spark;

    public CompilerExample(SparkSession spark) {
        this.spark = spark;
    }

    public void AggregateExpressionExample(Dataset<Row> df) {

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

    public void AggregateDynamicExample(Dataset<Row> df) {
        String groupByColName="country";
        String temptableName="users";
        List<Map<String, String>> components = new LinkedList<>();
        Map<String, String> component1 = new HashMap<>();
        component1.put("action", "sum");
        component1.put("colName", "age");
        component1.put("targetColName", "sumAge");

        Map<String, String> component2 = new HashMap<>();
        component2.put("action", "min");
        component2.put("colName", "age");
        component2.put("targetColName", "minAge");

        Map<String, String> component3 = new HashMap<>();
        component3.put("action", "sum");
        component3.put("colName", "weight");
        component3.put("targetColName", "avgWeight");

        Map<String, String> component4 = new HashMap<>();
        component4.put("action", "max");
        component4.put("colName", "age");
        component4.put("targetColName", "maxAge");

        components.add(component1);
        components.add(component2);
        components.add(component3);
        components.add(component4);
        String full_agg_expr = "";
        for (int i = 0; i < components.size(); i++) {
            Map<String, String> element = components.get(i);

            Object[] params = new Object[]{element.get("action"), element.get("colName"), element.get("targetColName")};
            String msg = MessageFormat.format("{0}({1}) as {2}", params);
            if (i == components.size() - 1) full_agg_expr = full_agg_expr + msg + " ";
            else full_agg_expr = full_agg_expr + msg + ", ";

        }
        Object[] params = new Object[]{groupByColName,full_agg_expr,temptableName,};
        String full_message=MessageFormat.format("Select {0}, {1} from {2} group by {0}",params);
        System.out.println(full_message);


        df.createOrReplaceTempView(temptableName);


        // corresponding spark query
        //  percentile_approx("age", 0.5, 10000).alias("medianAge"),
        Dataset<Row> result = spark.sql(full_message);
        result.show();
    }

    public static void main(String[] args) {
        //prepare spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark VTL example")
                .getOrCreate();
        // read csv file
        String path = "/home/pliu/git/VTLExample/data/users.csv";
        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv(path);
        df.show();
        df.printSchema();
        CompilerExample ce = new CompilerExample(spark);
        ce.AggregateDynamicExample(df);

    }
}
