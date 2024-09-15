package com.bilik.ditto.core.io;

import com.google.protobuf.Message;

import java.nio.file.Path;

public record FileRWInfo<T> (
        Class<? extends T> clasz,
        Path parentPath,
        String fileExtension
) {

    public static FileRWInfo<String> jsonFileInfo(Path parentPath) {
        return new FileRWInfo<>(String.class, parentPath, "json");
    }

    public static <T> FileRWInfo<T> tempFileInfo(Class<T> clasz, Path parentPath) {
        return new FileRWInfo<>(clasz, parentPath, "temp");
    }

    public static FileRWInfo<Message> protoFileInfo(Class<? extends Message> clasz, Path parentPath) {
        return new FileRWInfo<>(clasz, parentPath, "parquet");
    }

    /**
     * just primitive, please put only correct input, which is path ended with file without extension!
     */
    public Path resolveFile(String childPath) {
        childPath = childPath + "." + fileExtension;
        return parentPath.resolve(childPath);
    }

}
