package com.myspring.learnspringbatch.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class MyWriter<T> extends FlatFileItemWriter<T> {
    private final Storage storage = StorageOptions.getDefaultInstance().getService();
    @Value("gs://parent_directory/sub_directory.dat")
    private Resource resource;
    private Blob blob;
    private WriteChannel writeChannel;
    private ExecutionContext executionContext;

    @Override
    public void write(List<? extends T> items) throws Exception {
        String lines = doWrite(items);
        byte[] bytes = lines.toString().getBytes();
        try {
            createBlob();
            writeChannel = blob.writer();
            writeChannel.write(ByteBuffer.wrap(bytes));
            // executionContext.put("WriteChannel", writeChannel);
            writeChannel.close();
        } catch (IOException e) {
            throw new WriteFailedException("Could not write data.  The file may be corrupt.", e);
        }
        // state.setLinesWritten(state.getLinesWritten() + items.size());
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    public void createBlob() {
        logger.info("Inside open MyWriter");
        // try {
        // parent_directory/sub_directory
        // String bucket = resource.getURI().getHost();
        // String fileName = resource.getURI().getPath().substring(1);
        String bucket = "parent_directory";
        String fileName = "sub_directory/" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "/testFile"
                + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()) + ".dat";// EEE, d " + "MMM yyyy HH:mm:ss
                                                                                        // Z
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, fileName).build();
        // storage.create(blobInfo);
        // if (blob == null) {
        blob = storage.create(blobInfo);
        // outputStream = ((WritableResource) resource).getOutputStream();

        // } catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    @Override
    public void update(ExecutionContext executionContext) {
    }

    @Override
    public void close() {
        super.close();
        String bucket = "parent_directory";
        Page<Blob> blobs = storage.list(bucket,
                Storage.BlobListOption
                        .prefix("sub_directory/" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + "/"),
                Storage.BlobListOption.currentDirectory());
        for (Blob blob : blobs.iterateAll()) {
            logger.info(blob.getName());
        }
        // try {
        // writeChannel.close();
        // } catch (IOException e) {
        // logger.error("Unable to close WriteChannel", e);
        // }
    }

    // public void setStorage(Storage storage) {
    // this.storage = storage;
    // }

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
    }
}
