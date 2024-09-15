package com.bilik.ditto.transformation.vw;

import com.alibaba.fastjson.JSON;

/**
 * This class is for ser/deserialization json
 * This is MAIN class of config json for vowpal line building.
 */
public class Config {

    private Feature[] features;
    private Type type;

    public enum Type {
        PROTO,
        HASHMAP
    }

    public Feature[] getFeatures() {
        return features;
    }

    public void setFeatures(Feature[] features) {
        this.features = features;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Build feature paths structure from string json.
     * Expects string in this format:
     *
     * @param stringJson
     */
    public static Config buildFeatureMapFromString(String stringJson) {
        return JSON.parseObject(stringJson, Config.class);
    }

    /**
     * This method converts data from proto file into VW line by configuration provided in construct.
     * All magic is happened in Feature.
     */
    public String buildLine(Object message) {
        StringBuilder stringBuilder = new StringBuilder();
        char previous = (char) -1;
        for (Feature vowpalFeature : getFeatures()) {
            switch (vowpalFeature.getFeatureType()) {
                case BINARY:
                case NON_BINARY:
                    // adding ns
                    if (previous == (char) -1 || vowpalFeature.getNamespace() != previous) {
                        stringBuilder.append("|");
                        stringBuilder.append(vowpalFeature.getNamespace());
                        previous = vowpalFeature.getNamespace();
                    }
                    stringBuilder.append(" ");
                    stringBuilder.append(vowpalFeature.getStringFeature(message));
                    break;
                case AUTO_FILLED:
                    stringBuilder.append(" ");
                    stringBuilder.append(vowpalFeature.getStringFeature(message));
                    break;
                case PREDICT_VALUE:
                    stringBuilder.append(vowpalFeature.getStringFeature(message));
                    break;
                default:break;
            }

        }
        return stringBuilder.toString();
    }
}