package com.bilik.ditto.transformation.vw;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.bilik.ditto.transformation.Utils;
import com.bilik.ditto.transformation.vw.TransformationProvider.TransformationMethodMapper;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * For simplicity there may be 2 kinds of transformation function
 * a) transformation function which takes message (`Object`) and array of VowpalFeatures
 *   - this function will combine VowpalFeatures based by implementation/definition of the function.
 *   - mostly used for transformCoecDouble, sum/multiply combination of some features etc.
 * b) transformation function which takes message (`Object`) and SINGLE instance of VowpalFeatures
 *   - this will be most common function used for transformation current VowpalFeature
 *   - for example transformStats, fromTimestampToHourInDay etc.
 */
public class Transformations {
    public static class MultiplyFloat implements TransformationMethodMapper<Object, Feature[], Float> {

        @Override
        public String name() {
            return "multiplyFloat";
        }

        @Override
        public Float transform(Object message, Feature[] features) {
            double result = 1.0;
            for (Feature feature: features) {
                result *= sanitize(message, feature, 0.0);
            }
            return (float)result;
        }
    }

    public static class SumFloat implements TransformationMethodMapper<Object, Feature[], Float> {

        @Override
        public String name() {
            return "sumFloat";
        }

        @Override
        public Float transform(Object message, Feature[] features) {
            double result = 0.0;
            for (Feature feature: features) {
                result +=  sanitize(message, feature, 0.0);
            }
            return (float)result;
        }
    }

    public static class BoolToInt implements TransformationMethodMapper<Object, Feature, Integer> {

        @Override
        public String name() {
            return "boolToInt";
        }

        @Override
        public Integer transform(Object message, Feature features) {
            boolean stat = sanitize(message, features, false);
            return stat ? 1 : 0;
        }
    }

    public static class ArrayIdsFlatten implements TransformationMethodMapper<Object, Object, String> {

        @Override
        public String name() {
            return "arrayIdsFlatten";
        }

        @Override
        public String transform(Object message, Object value) {
            if (value instanceof Object[]) {
                Object[]v = (Object[]) value;
                return Arrays.stream(v).map(Object::toString).collect(Collectors.joining(","));
            } else if (value instanceof List) {
                List<Object> l = (List)value;
                return l.stream().map(Object::toString).collect(Collectors.joining(","));
            }
            return "";
        }
    }

    // HELPERS
    static <T> T sanitize(Object message, Feature feature, T defaultValue) {
        Object obj = feature.getValue(message);
        if (obj == null) {
            return defaultValue;
        } else {
            return (T)obj;
        }
    }

}
