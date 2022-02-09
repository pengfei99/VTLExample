import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class ScriptEngineExample {
    private final String engineName;
    private final ScriptEngine engine;

    public String getEngineName() {
        return engineName;
    }

    public ScriptEngine getEngine() {
        return engine;
    }

    public ScriptEngineExample(String engineName){
        this.engineName=engineName;
        // create a script engine manager
        ScriptEngineManager factory = new ScriptEngineManager();
        // create a JavaScript engine
        this.engine = factory.getEngineByName(engineName);
    }

    public void evalStr(String input) throws ScriptException {
        // evaluate JavaScript code from String
        this.engine.eval(input);
    }

    public void evalFile(String filePath) throws FileNotFoundException, ScriptException {
        FileReader fileContent = new FileReader(filePath);
        this.engine.eval(fileContent);
    }

    public static void main(String[] args){
        ScriptEngineExample se = new ScriptEngineExample("JavaScript");
        //eval string js code
        String jsCode = "print('Hello, World')";
        try{
            se.evalStr(jsCode);
        } catch (ScriptException e) {
            e.printStackTrace();
        }

        //eval js file code
        String codeFilePath="/home/pliu/git/VTLExample/data/test.js";
        try {
            se.evalFile(codeFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ScriptException e) {
            e.printStackTrace();
        }
    }

}
