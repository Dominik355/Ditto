package com.bilik.ditto.core.job.output.accumulation;

import com.bilik.ditto.core.io.PathWrapper;

import java.io.Closeable;

public interface FileUploader extends Closeable {

     boolean upload(PathWrapper path);

}
