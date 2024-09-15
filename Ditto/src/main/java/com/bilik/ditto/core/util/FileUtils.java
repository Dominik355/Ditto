package com.bilik.ditto.core.util;

import com.bilik.ditto.core.util.FileVisitors.CopyingFileVisitor;
import com.bilik.ditto.core.util.FileVisitors.DeletingFileVisitor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.bilik.ditto.core.util.FileVisitors.ALWAYS_TRUE;
import static com.bilik.ditto.core.util.FileVisitors.DEFAULT_COPY_OPTIONS;
import static java.nio.file.Files.*;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

public final class FileUtils {

    public static FileChannel newCreateWriteChannel(Path path) throws IOException {
        if (notExists(path.getParent())) {
            createDirectories(path.getParent());
        }
        return FileChannel.open(path, CREATE_NEW, WRITE);
    }

    public static void write(FileChannel channel, String string) throws IOException {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(bytes.length);
        buf.put(bytes);
        buf.flip();
        writeAll(channel, buf);
    }

    public static void writeAll(FileChannel channel, ByteBuffer src) throws IOException {
        long bytesToWrite = src.limit() - src.position();
        int bytesWritten;
        while ((bytesToWrite -= bytesWritten = channel.write(src)) > 0) {
            if (bytesWritten <= 0) {
                throw new IOException("Unable to write, reported bytes written: " + bytesWritten);
            }
        }
    }

    public static void delete(Path path) throws IOException {
        if (isDirectory(path)) {
            deleteDirectory(path);
        } else {
            deleteFile(path);
        }
    }

    public static void deleteDirectory(Path path) throws IOException {
        if (notExists(path)) {
            return;
        }
        if (!isDirectory(path)) {
            throw new NotDirectoryException(path.toString());
        }
        walkFileTree(path, new DeletingFileVisitor());
    }

    public static void deleteFile(Path file) throws IOException {
        if (notExists(file)) {
            return;
        }
        Files.delete(file);
    }

    public static boolean isDirectoryEmpty(Path directory) throws IOException {
        try (DirectoryStream<Path> dirStream = newDirectoryStream(directory)) {
            return !dirStream.iterator().hasNext();
        }
    }

    /**
     * Directory specific
     */
    public static void moveDirectory(Path toMove, Path target) throws IOException {
        createDirectories(target);
        copyDirectory(toMove, target);
        deleteDirectory(toMove);
    }

    /**
     * Moves both file and directory
     */
    public static void moveFile(Path toMove, Path target) throws IOException {
        requireExistingFile(toMove);
        if (exists(target)) {
            throw new FileAlreadyExistsException(target.toString());
        }

        try {
            move(toMove, target, REPLACE_EXISTING);
        } catch (IOException e) {
            if (isDirectory(toMove)) {
                copyDirectory(toMove, target);
                deleteDirectory(toMove);
            } else {
                copyFile(toMove, target);
                deleteFile(toMove);
            }
        }
    }

    public static void copyFile(Path srcFile, Path dstFile) throws IOException {
        createDirectories(dstFile.getParent());
        Files.copy(srcFile, dstFile, DEFAULT_COPY_OPTIONS);
    }

    public static void copyDirectory(Path from, Path to) throws IOException {
        createDirectories(to);
        copyDirectory(from, to, ALWAYS_TRUE);
    }

    public static void copyDirectory(Path from, Path to, Predicate<Path> filter) throws IOException {
        requireNonNull(from);
        requireNonNull(to);
        checkArgument(from.isAbsolute(), "From directory must be absolute");
        checkArgument(to.isAbsolute(), "To directory must be absolute");
        checkArgument(isDirectory(from), "From is not a directory");
        checkArgument(!from.normalize().equals(to.normalize()), "From and to directories are the same");

        if (notExists(to.getParent())) {
            createDirectories(to.getParent());
        }
        walkFileTree(from, new CopyingFileVisitor(from, to, filter, DEFAULT_COPY_OPTIONS));
    }

    public static Path requireExistingFile(final Path file) throws NoSuchFileException {
        if (!isRegularFile(file)) {
            throw new IllegalArgumentException("Parameter is not a file: " + file);
        }
        if (notExists(file)) {
            throw new NoSuchFileException(file.toString());
        }
        return file;
    }

    public static List<Path> listFiles(final Path file) throws IOException {
        return Files.walk(file)
                .filter(Files::isRegularFile)
                .toList();
    }

}
