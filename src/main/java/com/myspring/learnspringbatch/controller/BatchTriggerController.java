package com.myspring.learnspringbatch.controller;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/batch")
public class BatchTriggerController {

        @Autowired
        private JobLauncher jobLauncher;

        @Autowired
        private Job job;
        private final Storage storage = StorageOptions.getDefaultInstance().getService();;

        @GetMapping("/trigger")
        @ResponseBody
        @SneakyThrows
        public ExitStatus triggerReadWriteBatchJob() {
                log.info("/batch/trigger hit");
                return jobLauncher
                                .run(job,
                                                new JobParametersBuilder()
                                                                .addString("start at",
                                                                                new SimpleDateFormat(
                                                                                                "EEE, d MMM yyyy HH:mm:ss Z")
                                                                                                .format(new Date())
                                                                                                + "readWriteStep")
                                                                .toJobParameters())
                                .getExitStatus();
        }

        @GetMapping("/getBlobNames")
        @ResponseBody
        @SneakyThrows
        public List<String> getBlobNames() {
                log.info("/batch/getBlobNames hit");
                String bucket = "parent_directory";
                List<String> listBlobNames = new ArrayList<>();
                Page<Blob> blobs = storage.list(bucket,
                                Storage.BlobListOption.prefix(
                                                "sub_directory/" + new SimpleDateFormat("yyyyMMdd").format(new Date())
                                                                + "/"),
                                Storage.BlobListOption.currentDirectory());
                for (Blob blob : blobs.iterateAll()) {
                        log.info(blob.getName());
                        listBlobNames.add(blob.getName());
                }
                Collections.sort(listBlobNames);
                return listBlobNames;
        }

        @GetMapping("/getBlobs")
        @ResponseBody
        @SneakyThrows
        public List<String> getBlobs() {
                log.info("/batch/getBlobs hit");
                String bucket = "parent_directory";
                List<Blob> listBlob = new ArrayList<>();
                List<String> listBlobNames = new ArrayList<>();
                Page<Blob> blobs = storage.list(bucket,
                                Storage.BlobListOption.prefix(
                                                "sub_directory/" + new SimpleDateFormat("yyyyMMdd").format(new Date())
                                                                + "/"),
                                Storage.BlobListOption.currentDirectory());
                for (Blob blob : blobs.iterateAll()) {
                        log.info(blob.getName());
                        listBlob.add(blob);
                }
                final class BlobComparator implements Comparator<Blob> {
                        @Override
                        public int compare(Blob blobA, Blob blobB) {
                                return blobA.getName().compareToIgnoreCase(blobB.getName());
                        }
                }
                // listBlob.sort(new BlobComparator());
                Collections.shuffle(listBlob);
                listBlob.stream().forEach(x->listBlobNames.add(x.getName()));
                return listBlobNames;
        }
}