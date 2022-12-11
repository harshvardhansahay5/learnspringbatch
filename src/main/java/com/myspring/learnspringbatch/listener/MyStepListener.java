package com.myspring.learnspringbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
public class MyStepListener extends StepExecutionListenerSupport {
    @Nullable
    @Override
    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        stepExecution.getJobExecution().setExecutionContext(stepExecution.getExecutionContext());
        return null;
    }
}