import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;


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


        result.explain(true);
    }

    public void AggregateDynamicExample(Dataset<Row> df, String groupByColName, List<Map<String, String>> components) {
        String temptableName = "users";
        String full_agg_expr = "";
        // simulate dynamic full spark sql query building in processingEngine.executeAggr()
        for (int i = 0; i < components.size(); i++) {
            Map<String, String> element = components.get(i);
            String action = element.get("action");
            Object[] params = new Object[]{action, element.get("colName"), element.get("targetColName")};
            String msg;
            if (action == "count") {
                msg = MessageFormat.format("{0}(*) as {2}", params);
            } else {
                msg = MessageFormat.format("{0}({1}) as {2}", params);
            }
            if (i == components.size() - 1) full_agg_expr = full_agg_expr + msg + " ";
            else full_agg_expr = full_agg_expr + msg + ", ";

        }
        Object[] params = new Object[]{groupByColName, full_agg_expr, temptableName,};
        String full_message = MessageFormat.format("Select {0}, {1} from {2} group by {0}", params);
        System.out.println(full_message);

        df.createOrReplaceTempView(temptableName);

        // corresponding spark query
        //  percentile_approx("age", 0.5, 10000).alias("medianAge"),
        spark.sql(full_message).explain(true);
        //Dataset<Row> result = spark.sql(full_message);



       //result.show(5);
    }

    public void joinIndividualAggExample(Dataset<Row> df, String groupByColName, List<Map<String, String>> components,String joinColName) {
        List<Dataset<Row>> dfs = new LinkedList<>();
        
        // here I simulate the logic of each componentExpressionVisitor.visit(groupFunctionCtx.expr()) in aggrClause.visitAggrClause()
        for (Map<String, String> component : components) {
            String action = component.get("action");
            String colName = component.get("colName");
            String targetColName = component.get("targetColName");
            // System.out.println(targetColName);
            RelationalGroupedDataset df_group = df.groupBy(groupByColName);
            switch (action) {
                case "min":
                    Dataset<Row> df_min = df_group.min(colName).alias(targetColName);
                    dfs.add(df_min);
                    break;
                case "max":
                    Dataset<Row> df_max = df_group.max(colName).alias(targetColName);
                    dfs.add(df_max);
                    break;
                case "avg":
                    Dataset<Row> df_avg = df_group.avg(colName).alias(targetColName);
                    dfs.add(df_avg);
                    break;
                case "sum":
                    Dataset<Row> df_sum = df_group.sum(colName).alias(targetColName);
                    dfs.add(df_sum);
                    break;
                case "count":
                    Dataset<Row> df_count = df_group.count().alias(targetColName);
                    dfs.add(df_count);
                    break;
                case "collect_list":
                    Dataset<Row> df_collect = df_group.agg(collect_list(colName).alias(targetColName));
                    dfs.add(df_collect);
                    break;
                default:
                    System.out.println("Unknown aggregation action");
            }
        }
        Dataset<Row> dfl = dfs.get(0);
        for (int i = 1; i < dfs.size(); i++) {
            Dataset<Row> dfr = dfs.get(i);
            dfl = dfl.join(dfr, joinColName);
        }
        dfl.explain(true);

    }

    public List<Map<String, String>> getActions(){
        List<Map<String, String>> components = new LinkedList<>();
        //agg function 1
        Map<String, String> component1 = new HashMap<>();
        component1.put("action", "sum");
        component1.put("colName", "age");
        component1.put("targetColName", "sumAge");

        //agg function 2
        Map<String, String> component2 = new HashMap<>();
        component2.put("action", "min");
        component2.put("colName", "age");
        component2.put("targetColName", "minAge");

        //agg function 3
        Map<String, String> component3 = new HashMap<>();
        component3.put("action", "avg");
        component3.put("colName", "weight");
        component3.put("targetColName", "avgWeight");


        //agg function 4
        Map<String, String> component4 = new HashMap<>();
        component4.put("action", "max");
        component4.put("colName", "age");
        component4.put("targetColName", "maxAge");

        //agg function 5
        Map<String, String> component5 = new HashMap<>();
        component5.put("action", "count");
        component5.put("colName", "*");
        component5.put("targetColName", "countVal");

        components.add(component1);
        components.add(component2);
        components.add(component3);
        components.add(component4);
        components.add(component5);

        return components;

    }


    public List<Map<String, String>> getActions1(){
        List<Map<String, String>> components = new LinkedList<>();
        //agg function 1
        Map<String, String> component1 = new HashMap<>();
        component1.put("action", "sum");
        component1.put("colName", "NumberofAlarms");
        component1.put("targetColName", "sumNumberofAlarms");

        //agg function 2
        Map<String, String> component2 = new HashMap<>();
        component2.put("action", "min");
        component2.put("colName", "NumberofAlarms");
        component2.put("targetColName", "minNumberofAlarms");

        //agg function 3
        Map<String, String> component3 = new HashMap<>();
        component3.put("action", "avg");
        component3.put("colName", "NumberofAlarms");
        component3.put("targetColName", "avgNumberofAlarms");


        //agg function 4
        Map<String, String> component4 = new HashMap<>();
        component4.put("action", "max");
        component4.put("colName", "NumberofAlarms");
        component4.put("targetColName", "NumberofAlarms");

        //agg function 5
        Map<String, String> component5 = new HashMap<>();
        component5.put("action", "count");
        component5.put("colName", "*");
        component5.put("targetColName", "countVal");

        components.add(component1);
        components.add(component2);
        components.add(component3);
        components.add(component4);
        components.add(component5);
        return components;
    }

    public List<Map<String, String>> getActions2(){
        List<Map<String, String>> components = new LinkedList<>();


        //agg function 5
        Map<String, String> component1 = new HashMap<>();
        component1.put("action", "count");
        component1.put("colName", "*");
        component1.put("targetColName", "countVal");

        components.add(component1);
        return components;
    }

    public List<Map<String, String>> getActions3(){
        List<Map<String, String>> components = this.getActions1();

        Map<String, String> component6 = new HashMap<>();
        component6.put("action", "collect_list");
        component6.put("colName", "City");
        component6.put("targetColName", "allCities");


        components.add(component6);
        return components;
    }

    public void performenceTest(Dataset<Row> df, List<Map<String, String>> components, String groupByColName){

        // simulate execute all aggregate actions with one single groupby without join in processingEngine.executeAggr()
        long startTime1 = System.nanoTime();
        this.AggregateDynamicExample(df, groupByColName, components);
        long endTime1 = System.nanoTime();
        long duration1 = (endTime1 - startTime1);
        System.out.println("Duration of AggregateDynamic: "+duration1+ " nano seconds");

        // simulate execute aggregate action in each visit then join the result in processingEngine.executeAggr()
        long startTime2 = System.nanoTime();
        this.joinIndividualAggExample(df, groupByColName, components, groupByColName);
        long endTime2 = System.nanoTime();
        long duration2=(endTime2-startTime2);
        System.out.println("Duration of joinIndividualAgg: "+duration2+ " nano seconds");

        long timeGain=(duration2-duration1)/1000000;

        System.out.println("Time gap: " + timeGain + " milli seconds" );
    }

    public static void main(String[] args) {
        //prepare spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark VTL example")
                .getOrCreate();

        CompilerExample ce = new CompilerExample(spark);

//        // read csv file
//        String path = "/home/pliu/git/VTLExample/data/users.csv";
//        Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true").csv(path);
//        df.show();
//        df.printSchema();
//        String groupByColName = "country";
//        List<Map<String, String>> actions = ce.getActions();
//        ce.performenceTest(df,actions,groupByColName);


        // read sf_fire parquet
        String sf_fire_path="/home/pliu/data_set/sf_fire";
        Dataset<Row> df1=spark.read().parquet(sf_fire_path);
        df1.show();
        df1.printSchema();
        long rowNumber=df1.count();
        System.out.println("Row number: "+rowNumber);

        String groupByColName1 = "CallType";

        // test case 1
//        List<Map<String, String>> actions2 = ce.getActions2();
//        ce.performenceTest(df1,actions2,groupByColName1);

        // test case 2
//        List<Map<String, String>> actions1 = ce.getActions1();
//        ce.performenceTest(df1,actions1,groupByColName1);

        // test case 3
        List<Map<String, String>> actions3 = ce.getActions3();
        ce.performenceTest(df1,actions3,groupByColName1);




    }
}
