package com.dnanexus;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * An analysis object (a specific instantiation of a workflow).
 */
public final class DXAnalysis extends DXExecution {

    /**
     * A response from the /analysis-xxxx/terminate route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class AnalysisTerminateResponse {}

    /**
     * Contains metadata about an analysis. All accessors reflect the state of the analysis at the
     * time that this object was created.
     */
    public final static class Describe extends DXExecution.Describe {
        private final DescribeResponseHash describeOutput;
        @SuppressWarnings("unused")
        private final DXEnvironment env;

        // TODO: executable
        // TODO: workflow

        // TODO: plus the common fields in DXExecution.Describe

        @VisibleForTesting
        Describe(DescribeResponseHash describeOutput, DXEnvironment env) {
            this.describeOutput = describeOutput;
            this.env = env;
        }

        @Override
        public String getId() {
            return describeOutput.id;
        }

        @Override
        public String getName() {
            return describeOutput.name;
        }

        @Override
        public <T> T getOutput(Class<T> outputClass) {
            return DXJSON.safeTreeToValue(describeOutput.output, outputClass);
        }

        @Override
        public Map<String, String> getProperties() {
            return ImmutableMap.copyOf(describeOutput.properties);
        }

        /**
         * Returns the state of the analysis.
         *
         * @return analysis state
         */
        public AnalysisState getState() {
            return describeOutput.state;
        }

        @Override
        public List<String> getTags() {
            return ImmutableList.copyOf(describeOutput.tags);
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
        private List<String> tags;
        @JsonProperty
        private Map<String, String> properties;

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
     * environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXAnalysis getInstanceWithCachedDescribe(String jobId, DXEnvironment env,
            JsonNode describe) {
        return new DXAnalysis(jobId, Preconditions.checkNotNull(env, "env may not be null"),
                Preconditions.checkNotNull(describe, "describe may not be null"));
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
        return new DXAnalysis(analysisId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    private DXAnalysis(String analysisId) {
        super(analysisId, null);
    }

    private DXAnalysis(String analysisId, DXEnvironment env) {
        super(analysisId, env);
    }

    private DXAnalysis(String analysisId, DXEnvironment env, JsonNode cachedDescribe) {
        super(analysisId, env, cachedDescribe);
    }

    private Describe describeImpl(JsonNode describeInput) {
        return new Describe(DXAPI.analysisDescribe(this.getId(), describeInput,
                DescribeResponseHash.class), this.env);
    }

    @Override
    public Describe describe() {
        return describeImpl(MAPPER.createObjectNode());
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return new Describe(
                DXJSON.safeTreeToValue(this.cachedDescribe, DescribeResponseHash.class), this.env);
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

    @Override
    public void terminate() {
        DXAPI.analysisTerminate(this.getId(), AnalysisTerminateResponse.class);
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
