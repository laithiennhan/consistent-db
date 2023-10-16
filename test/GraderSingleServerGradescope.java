import com.gradescope.jh61b.grader.GradedTestListenerJSON;
import org.junit.*;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.io.IOException;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GraderSingleServerGradescope extends GraderSingleServer {

    public static void main(String[] args) throws IOException {
        JUnitCore runner = new JUnitCore();
        runner.addListener(new GradedTestListenerJSON());
        Result r = runner.run(GraderSingleServer.class);

//        Result result = JUnitCore.runClasses(GraderSingleServerGradescope.class);
//        for (Failure failure : result.getFailures()) {
//            System.out.println(failure.toString());
//            failure.getException().printStackTrace();
//        }
    }
}