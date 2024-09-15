package com.bilik.ditto.transformation.vw;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Singleton with lazy initialization, in case dependency is not used in project.
 */
public class TransformationProvider {

    public final Map<String, TransformationMethodMapper<?,?,?>> allFunctions;

    private TransformationProvider() {
        allFunctions = Arrays.stream(new TransformationMethodMapper<?, ?, ?>[] {
                        new Transformations.TransformCoecDouble(),
                        new Transformations.TransformStats(),
                        new Transformations.FromTimestampToHourInDay(),
                        new Transformations.BinaryCoecFloat(),
                        new Transformations.MultiplyFloat(),
                        new Transformations.SumFloat(),
                        new Transformations.BoolToInt(),
                        new Transformations.ArrayIdsFlatten()})
                .collect(Collectors.toMap(
                        TransformationMethodMapper::name,
                        Function.identity()
                ));
    }

    private static class LazySingleton {
        static final TransformationProvider instance = new TransformationProvider();
    }

    public static TransformationProvider getInstance() {
        return LazySingleton.instance;
    }

    public void addTransformFunction(TransformationMethodMapper<?,?,?> transformation) {
        allFunctions.put(transformation.name(), transformation);
    }



    public BiFunction getTransformFunction(String methodName) {
        return allFunctions.get(methodName).function();
    }

    public interface TransformationMethodMapper<Message, Attr, Ret> {

        String name();

        Ret transform(Message m, Attr a);

        default BiFunction<Message, Attr, Ret> function() {
            return this::transform;
        }

    }

}