package com.dnanexus;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * An analysis object (a specific instantiation of a workflow).
 */
public class DXAnalysis extends DXExecution {

    /**
     * Contains metadata about an analysis. All accessors reflect the state of the analysis at the
     * time that this object was created.
     */
    public final static class Describe {
        private final DescribeResponseHash describeOutput;

        // TODO: lots more fields from /analysis-xxxx/describe

        @VisibleForTesting
        Describe(DescribeResponseHash describeOutput) {
            this.describeOutput = describeOutput;
        }

        /**
         * Returns the ID of the analysis.
         *
         * @return the analysis ID
         */
        public String getId() {
            return describeOutput.id;
        }

        /**
         * Returns the name of the analysis.
         *
         * @return the analysis name
         */
        public String getName() {
            return describeOutput.name;
        }

        /**
         * Returns the output of the analysis, deserialized to the specified class.
         *
         * <p>
         * Note that this field is not guaranteed to be complete until the analysis has reached
         * state {@link AnalysisState#DONE}. However, partial outputs might be populated before that
         * time.
         * </p>
         *
         * @param outputClass
         *
         * @return analysis output object
         */
        public <T> T getOutput(Class<T> outputClass) {
            return DXJSON.safeTreeToValue(describeOutput.output, outputClass);
        }

        /**
         * Returns the state of the analysis.
         *
         * @return analysis state
         */
        public AnalysisState getState() {
            return describeOutput.state;
        }
    }

    /**
     * Deserialized output from the /analysis-xxxx/describe route. Not directly accessible by users
     * (see Describe instead).
     */
    @VisibleForTesting
    @JsonIgnoreProperties(ignoreUnknown = true)
    final static class DescribeResponseHash {
        @JsonProperty
        private String id;
        @JsonProperty
        private String name;
        @JsonProperty
        private AnalysisState state;

        @JsonProperty
        private JsonNode output;
    }

    private static final Set<AnalysisState> unsuccessfulAnalysisStates = Sets.immutableEnumSet(
            AnalysisState.FAILED, AnalysisState.TERMINATED);

    /**
     * Returns a {@code DXAnalysis} representing the specified analysis.
     *
     * @param analysisId Analysis ID, of the form {@code "analysis-xxxx"}
     *
     * @return a {@code DXAnalysis}
     *
     * @throws NullPointerException If {@code analysisId} is null
     */
    public static DXAnalysis getInstance(String analysisId) {
        return new DXAnalysis(analysisId);
    }

    /**
     * Returns a {@code DXAnalysis} representing the specified analysis using the specified
     * environment.
     *
     * @param analysisId Analysis ID, of the form {@code "analysis-xxxx"}
     * @param env environment to use for making subsequent API calls
     *
     * @return a {@code DXAnalysis}
     *
     * @throws NullPointerException If {@code analysisId} or {@code env} is null
     */
    public static DXAnalysis getInstanceWithEnvironment(String analysisId, DXEnvironment env) {
        Preconditions.checkNotNull(env);
        return new DXAnalysis(analysisId, env);
    }

    private DXAnalysis(String analysisId) {
        super(analysisId, null);
    }

    private DXAnalysis(String analysisId, DXEnvironment env) {
        super(analysisId, env);
    }

    private Describe describeImpl(JsonNode describeInput) {
        return new Describe(DXAPI.analysisDescribe(this.getId(), describeInput,
                DescribeResponseHash.class));
    }

    /**
     * Obtains information about the analysis.
     *
     * @return a {@code Describe} containing analysis metadata
     */
    public Describe describe() {
        return describeImpl(MAPPER.createObjectNode());
    }

    @Override
    public <T> T getOutput(Class<T> outputClass) throws IllegalStateException {
        // {fields: {output: true, state: true}}
        Describe d =
                describeImpl(DXJSON
                        .getObjectBuilder()
                        .put("fields",
                                DXJSON.getObjectBuilder().put("output", true).put("state", true)
                                        .build()).build());
        if (d.getState() != AnalysisState.DONE) {
            throw new IllegalStateException(
                    "Expected analysis to be in state DONE, but it is in state " + d.getState());
        }
        return d.getOutput(outputClass);
    }

    /**
     * Waits until the analysis has successfully completed and is in the DONE state.
     *
     * @return the same DXAnalysis object
     *
     * @throws IllegalStateException if the analysis reaches the FAILED or TERMINATED state
     */
    @Override
    public DXAnalysis waitUntilDone() throws IllegalStateException {
        AnalysisState analysisState = this.describe().getState();
        while (analysisState != AnalysisState.DONE) {
            if (unsuccessfulAnalysisStates.contains(analysisState)) {
                throw new IllegalStateException(this.getId() + " is in unsuccessful state "
                        + analysisState.toString());
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            analysisState = this.describe().getState();
        }
        return this;
    }

}
