import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import javax.script.*;
import java.util.List;
import java.util.Map;

public class VTLEvaluator {


    public static void main(String[] args) throws ScriptException {


        // create a script engine manager
        ScriptEngineManager engineFac = new ScriptEngineManager();
        // create a vtl engine
        ScriptEngine engine = engineFac.getEngineByName("vtl");

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Alice", "sex", "F", "age", 25L),
                        Map.of("name", "Bob", "sex", "M", "age", 30L),
                        Map.of("name", "Charlie", "sex", "M", "age", 5L),
                        Map.of("name", "toto", "sex", "M", "age", -1L),
                        Map.of("name", "titi", "sex", "M", "age", 118L)
                ),
                Map.of("name", String.class, "sex", String.class, "age", Long.class),
                Map.of("name", Dataset.Role.IDENTIFIER,"sex", Dataset.Role.MEASURE,"age", Dataset.Role.MEASURE)

        );

        // load dataset into binding
        Bindings bindings = new SimpleBindings();
        bindings.put("users", dataset);

        // add bindings(dataset) to engine context
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        String query1= "ageValAnomalies := users[filter age < 0];";
        String query2= "age_null_val := users[filter isnull(age)];";
        String query3= "ageValAnomalies := users[filter age < 0 and age >100];";
        // define a validation rule on dataset, column age can't have null values
        // The keyword check is not implemented for now. So we can't test below query.
        // For more information about VTL https://inseefr.github.io/Trevas/en/coverage.html
        String validation_rule = "res := check(users[filter not isnull(age)] >= 1 " +
                "errorcode \"Values, when provided, should be higher or equal to 1\"\n" +
                "errorlevel \"Warning\");";

        // apply query1 et query2
        try {
            engine.eval(query1);
            //engine.eval(query2);
            //engine.eval(query3);
        } catch (ScriptException e) {
            e.printStackTrace();
        }

        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

        InMemoryDataset ageValAnomalies = (InMemoryDataset) outputBindings.get("ageValAnomalies");

        List<List<Object>> ageValAnomaliesList = ageValAnomalies.getDataAsList();
        System.out.println("Column age value anomalies: "+ageValAnomaliesList);

//        InMemoryDataset ageNullVal = (InMemoryDataset) outputBindings.get("age_null_val");
//        List<List<Object>> ageNullValList = ageNullVal.getDataAsList();
//        System.out.println("Column age value anomalies: "+ageNullValList);


    }

}
