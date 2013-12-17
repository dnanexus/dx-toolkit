import java.io.IOException;

import com.dnanexus.DXJSON;
import com.dnanexus.DXUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

public class DXHelloWorld {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class HelloWorldInput {
        @JsonProperty
        private String name;
    }

    private static class HelloWorldOutput {
        @JsonProperty
        private String greeting;

        public HelloWorldOutput(String greeting) {
            this.greeting = greeting;
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("This is the DNAnexus Java Demo App");

        HelloWorldInput input = DXUtil.getJobInput(HelloWorldInput.class);

        String name = input.name;
        String greeting = "Hello, " + (name == null ? "World" : name) + "!";

        DXUtil.writeJobOutput(new HelloWorldOutput(greeting));
    }

}
