package com.myspring.learnspringbatch.controller;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@RestController
@RequestMapping("/batch")
public class BatchTriggerController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @GetMapping("/trigger")
    @ResponseBody
    @SneakyThrows
    public Boolean triggerReadWriteBatchJob() {
        jobLauncher.run(job, new JobParametersBuilder().addString("start at", new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").format(new Date()) + "readWriteStep")
                .toJobParameters());
        return true;
    }
}