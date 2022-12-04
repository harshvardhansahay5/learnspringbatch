package com.myspring.learnspringbatch.jobconfig;

import com.myspring.learnspringbatch.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.text.SimpleDateFormat;
import java.util.Date;

@Configuration
@EnableBatchProcessing
public class BatchJobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final FileSystemResource inputResource;
    private final FileSystemResource outputResource;

    public BatchJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, @Value("src/main/resources/mock_data.csv") FileSystemResource inputResource, @Value("src/main/resources/mock_data.dat") FileSystemResource outputResource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.inputResource = inputResource;
        this.outputResource = outputResource;
    }

    @Bean
    public Job readWriteJob() {
        return jobBuilderFactory.get(new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").format(new Date()) + "readWriteJob")
                .start(readWriteStep())
                .build();
    }

    @Bean
    public Step readWriteStep() {
        return stepBuilderFactory.get(new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").format(new Date()) + "readWriteStep")
                .<Customer, Customer>chunk(10)
                .reader(customerItemReader())
                .processor(customerItemProcessor())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public ItemReader<Customer> customerItemReader() {
        FlatFileItemReader<Customer> customerFlatFileItemReader = new FlatFileItemReader<>();

        DefaultLineMapper<Customer> customerDefaultLineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames("id", "first_name", "last_name", "email", "gender", "ip_address");

        BeanWrapperFieldSetMapper<Customer> customerBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        customerBeanWrapperFieldSetMapper.setTargetType(Customer.class);

        customerDefaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        customerDefaultLineMapper.setFieldSetMapper(customerBeanWrapperFieldSetMapper);

        customerFlatFileItemReader.setResource(inputResource);
        customerFlatFileItemReader.setLinesToSkip(1);
        customerFlatFileItemReader.setLineMapper(customerDefaultLineMapper);

        return customerFlatFileItemReader;
    }

    @Bean
    public ItemProcessor<Customer, Customer> customerItemProcessor() {
        return customer -> customer;
    }

    @Bean
    public ItemWriter<Customer> customerItemWriter() {
        FlatFileItemWriter<Customer> customerFlatFileItemWriter = new FlatFileItemWriter<>();

        DelimitedLineAggregator<Customer> customerDelimitedLineAggregator = new DelimitedLineAggregator<>();

        BeanWrapperFieldExtractor<Customer> customerBeanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        customerBeanWrapperFieldExtractor.setNames(new String[]{"id", "firstName", "lastName", "email", "gender", "ipAddress"});

        customerDelimitedLineAggregator.setDelimiter("|");
        customerDelimitedLineAggregator.setFieldExtractor(customerBeanWrapperFieldExtractor);

        customerFlatFileItemWriter.setResource(outputResource);
        customerFlatFileItemWriter.setAppendAllowed(false);
        customerFlatFileItemWriter.setShouldDeleteIfExists(true);
        customerFlatFileItemWriter.setLineAggregator(customerDelimitedLineAggregator);
        customerFlatFileItemWriter.setHeaderCallback(headerWriter -> headerWriter.write("id|first_name|last_name" + "|email|gender|ip_address"));
        customerFlatFileItemWriter.setFooterCallback(footerWriter -> footerWriter.write(new SimpleDateFormat("EEE, d " + "MMM yyyy HH:mm:ss Z").format(new Date())));
        return customerFlatFileItemWriter;
    }
}