package com.bilik.ditto.testCommons;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class TestUtils {

    static void fail() {
        fail(null);
    }

    static void fail(String message) {
        if (message == null) {
            throw new AssertionError();
        }
        throw new AssertionError(message);
    }

    public static void deleteDirectory(String first, String... more) throws IOException {
        Path directory = Path.of(first, more);

        if (Files.exists(directory)) {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                    System.out.println("Deleting file: " + path);
                    Files.delete(path);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException ioException) throws IOException {
                    System.out.println("Deleting dir: " + directory);
                    if (ioException == null) {
                        Files.delete(directory);
                        return FileVisitResult.CONTINUE;
                    } else {
                        throw ioException;
                    }
                }
            });
        }
    }

}
