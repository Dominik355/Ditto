package com.bilik.ditto.core.util;

import java.util.ArrayList;
import java.util.List;

public class CollectionUtils {

    public static <T> List<List<T>> toNParts(List<T> list, int N) {
        float partSize = list.size() / (float) N;
        List<List<T>> result = new ArrayList<>(N);
        for (int currentPart = 0; currentPart < N; currentPart++) {
            result.add(currentPart, list.subList(
                    (int) Math.ceil(currentPart * partSize),
                    (int) Math.ceil(currentPart * partSize + partSize)));
        }

        return result;
    }

    public static <T> List<T> getFirstN(List<T> list, int n) {
        if (n >= list.size()) {
            return new ArrayList<>(list);
        }
        return list.subList(0, n);
    }

}
