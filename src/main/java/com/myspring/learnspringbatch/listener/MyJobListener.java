package com.myspring.learnspringbatch.listener;

import java.io.IOException;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;

import com.google.cloud.WriteChannel;

@Component
public class MyJobListener extends JobExecutionListenerSupport {

    @Override
    @AfterStep
    public void afterJob(JobExecution jobExecution) {
        // try {
        //     ((WriteChannel) (jobExecution.getExecutionContext().get("WriteChannel"))).close();
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }
}