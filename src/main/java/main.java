import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;

import javax.script.*;
import java.util.List;
import java.util.Map;

public class main {

    public static void main(String[] args) throws ScriptException {

        // create a script engine manager
        ScriptEngineManager engineFac = new ScriptEngineManager();
        // create a vtl engine
        ScriptEngine engine = engineFac.getEngineByName("vtl");


         // create a dataset
        // By default, if a variable role is not defined, the `MEASURE` will be affected.
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Alice", "sex", "F", "age", 25L),
                        Map.of("name", "Bob", "sex", "M", "age", 30L),
                        Map.of("name", "Charlie", "sex", "M", "age", 5L)
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

        // define a validation rule on dataset, column age can't have null values
        String transformation= "res := users[filter age > 6];";
        String validation_rule = "res := check(users[filter not isnull(age)] >= 1 " +
                "errorcode \"Values, when provided, should be higher or equal to 1\"\n" +
                "errorlevel \"Warning\");";

        // apply validation rule a
        try {
            engine.eval(transformation);
        } catch (ScriptException e) {
            e.printStackTrace();
        }

        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

        InMemoryDataset res = (InMemoryDataset) outputBindings.get("res");

        List<List<Object>> resList = res.getDataAsList();
        System.out.println(resList);


    }

}
