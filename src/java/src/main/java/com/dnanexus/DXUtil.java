package com.dnanexus;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for handling job input and output.
 */
public class DXUtil {

    /**
     * Working directory of the applet. This is the directory where the job_input.json and
     * job_output.json files are to be found. We record this once at application startup to protect
     * ourselves against possible subsequent changes to the working directory.
     */
    private static final String startupDirectory;

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        startupDirectory = System.getProperty("user.dir");
    }


    /**
     * Obtains the job input and deserializes it to the specified class.
     *
     * @param valueType class to deserialize job input to
     *
     * @return job input
     */
    public static <T> T getJobInput(Class<T> valueType) {
        String jobInputPath = startupDirectory + "/job_input.json";
        try {
            return DXJSON.safeTreeToValue(mapper.readTree(new File(jobInputPath)), valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes the job output.
     *
     * @param object object to be JSON serialized
     */
    public static void writeJobOutput(Object object) {
        String jobOutputPath = startupDirectory + "/job_output.json";
        try {
            mapper.writeValue(new File(jobOutputPath), object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
