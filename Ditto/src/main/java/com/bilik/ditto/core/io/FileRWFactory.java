package com.bilik.ditto.core.io;

public interface FileRWFactory<T> {

    FileReader<T> buildFileReader(PathWrapper path) throws Exception;

    FileWriter<T> buildFileWriter(PathWrapper path) throws Exception;

    FileRWInfo<T> getInfo();

}
