package com.bilik.ditto.transformation.vw;

/**
 * This class is for ser/deserialization from json
 */
public class TransformationDefinition {

    private String function;
    private Feature[] others;
    private String selectors;

    public TransformationDefinition(String function, Feature[] others) {
        this(function, others, null);
    }

    public TransformationDefinition(String function, Feature[] others, String selectors) {
        this.function = function;
        this.others = others;
        this.selectors = selectors;
    }

    public String getFunction() {
        return function;
    }

    public Feature[] getOthers() {
        return others;
    }

    public String getSelectors() {
        return selectors;
    }

}