import com.fasterxml.jackson.core.type.TypeReference;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.script.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VTLSparkEvaluator {

    public static void main(String[] args) {
        //prepare spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("Spark VTL example")
                .getOrCreate();
        // read csv file
        String path = "/home/pliu/git/VTLExample/data/flight.csv";
        // build csv schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("DEST_COUNTRY_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("FLI_NUM", DataTypes.LongType, true));
        StructType schema= DataTypes.createStructType(fields);
        Dataset<Row> df = spark.read().option("header", "True").option("inferSchema", "False").schema(schema).csv(path);
        df.show();
        df.printSchema();

        // prepare script engine
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();

        // prepare bindings
        Map<String, fr.insee.vtl.model.Dataset.Role> roles = new HashMap<>();
        roles.put("DEST_COUNTRY_NAME", fr.insee.vtl.model.Dataset.Role.MEASURE);
        roles.put("ORIGIN_COUNTRY_NAME", fr.insee.vtl.model.Dataset.Role.MEASURE);
        roles.put("FLI_NUM", fr.insee.vtl.model.Dataset.Role.MEASURE);


        SparkDataset dataset = new SparkDataset(df, roles);

        Bindings bindings = new SimpleBindings();
        bindings.put("flights", dataset);

        // add binding to context
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        // set up spark as engine
        engine.put("$vtl.engine.processing_engine_names", "spark");
        engine.put("$vtl.spark.session", spark);


        //
        String query1 = "res := flights[filter FLI_NUM < 5 ];";
        try {
            engine.eval(query1);
            //engine.eval(query2);
            //engine.eval(query3);
        } catch (ScriptException e) {
            e.printStackTrace();
        }

        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

        InMemoryDataset res = (InMemoryDataset) outputBindings.get("res");

        List<List<Object>> ageValAnomaliesList = res.getDataAsList();
        System.out.println("res: " + ageValAnomaliesList);

    }
}
