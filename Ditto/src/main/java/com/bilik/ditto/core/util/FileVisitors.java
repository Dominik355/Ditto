package com.bilik.ditto.core.util;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.list;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileVisitors {

    public static final CopyOption[] DEFAULT_COPY_OPTIONS = {REPLACE_EXISTING, COPY_ATTRIBUTES};
    public static final Predicate<Path> ALWAYS_TRUE = p -> true;

    /**
     * Deletes all files allowed by predicate. If directory ends up empty, deletes it too.
     */
    public static class DeletingFileVisitor extends SimpleFileVisitor<Path> {

        private final Predicate<Path> removeFilePredicate;
        private int skippedFiles;

        public DeletingFileVisitor() {
            this.removeFilePredicate = ALWAYS_TRUE;
        }

        public DeletingFileVisitor(Predicate<Path> removeFilePredicate) {
            this.removeFilePredicate = removeFilePredicate;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (removeFilePredicate.test(file)) {
                FileUtils.delete(file);
            } else {
                skippedFiles++;
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
            if (e != null) {
                throw e;
            }
            try {
                if (skippedFiles == 0 || FileUtils.isDirectoryEmpty(dir)) {
                    FileUtils.delete(dir);
                }
                return CONTINUE;
            } catch (DirectoryNotEmptyException notEmpty) {
                String reason = notEmptyReason(dir, notEmpty);
                throw new IOException(notEmpty.getMessage() + ": " + reason, notEmpty);
            }
        }

        private static String notEmptyReason(Path dir, DirectoryNotEmptyException notEmpty) {
            try (Stream<Path> list = list(dir)) {
                return list.map(p -> String.valueOf(p.getFileName()))
                        .collect(Collectors.joining("', '", "'", "'."));
            } catch (Exception e) {
                notEmpty.addSuppressed(e);
                return "(could not list directory: " + e.getMessage() + ")";
            }
        }
    }

    public static class CopyingFileVisitor extends SimpleFileVisitor<Path> {

        private final Path from;
        private final Path to;
        private final Predicate<Path> filter;
        private final CopyOption[] copyOption;
        private final Set<Path> copiedPathsInDestination = new HashSet<>();

        public CopyingFileVisitor(Path from, Path to, CopyOption... copyOption) {
            this.from = from.normalize();
            this.to = to.normalize();
            this.filter = ALWAYS_TRUE;
            this.copyOption = copyOption;
        }

        public CopyingFileVisitor(Path from, Path to, Predicate<Path> filter, CopyOption... copyOption) {
            this.from = from.normalize();
            this.to = to.normalize();
            this.filter = filter;
            this.copyOption = copyOption;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (!from.equals(dir) && !filter.test(dir)) {
                return SKIP_SUBTREE;
            }

            if (copiedPathsInDestination.contains(dir)) {
                return SKIP_SUBTREE;
            }

            Path target = to.resolve(from.relativize(dir));
            if (!exists(target)) {
                createDirectory(target);
                if (isInDestination(target)) {
                    copiedPathsInDestination.add(target);
                }
            }
            return CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (!filter.test(file)) {
                return CONTINUE;
            }
            if (!copiedPathsInDestination.contains(file)) {
                Path target = to.resolve(from.relativize(file));
                Files.copy(file, target, copyOption);
                if (isInDestination(target)) {
                    copiedPathsInDestination.add(target);
                }
            }
            return CONTINUE;
        }

        private boolean isInDestination(Path path) {
            return path.startsWith(to);
        }
    }

}
