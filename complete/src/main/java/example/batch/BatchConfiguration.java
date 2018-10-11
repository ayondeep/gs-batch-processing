package example.batch;

import javax.sql.DataSource;

import example.domain.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.integration.partition.BeanFactoryStepLocator;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.PollableChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration implements ApplicationContextAware {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    JobExplorer jobExplorer;

    private ApplicationContext applicationContext;

    final static int GRID_SIZE = 4;

    // ------------------------- SETUP JOB  ------------------
    /**
     * Reads data from CSV file.
     * This is part of initial setup
     * @return
     */
    @StepScope
    @Bean
    public FlatFileItemReader<Person> setupReader() {
        return new FlatFileItemReaderBuilder<Person>()
            .name("personItemReader")
            .resource(new ClassPathResource("sample-data.csv"))
            .delimited()
            .names(new String[]{"firstName", "lastName"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }})
            .build();
    }


    /**
     * Writes data to Person table.
     * This is part of initial setup.
     * @param dataSource
     * @return
     */
    @StepScope
    @Bean
    public JdbcBatchItemWriter<Person> setupWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO person (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }

    /**
     * Initial setup job. This job will read person names from CSV file and enter into Person table.
     * @param listener
     * @param setupStep
     * @return
     */
    @Bean
    @Profile("master")
    public Job setupUserJob(JobCompletionNotificationListener listener, Step setupStep) {
        return jobBuilderFactory.get("setupUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(setupStep)
                .end()
                .build();
    }


    @Bean
    public Step setupStep(JdbcBatchItemWriter<Person> setupWriter) {
        return stepBuilderFactory.get("setupStep")
                .<Person, Person> chunk(10)
                .reader(setupReader())
                .writer(setupWriter)
                //.taskExecutor(new SimpleAsyncTaskExecutor()).throttleLimit(10)
                .build();
    }

    // --------------------------------- Transformer Job using Partitioning -------------------

    /**
     * Reads data from Person table.
     * @param dataSource
     * @return
     * @throws Exception
     */
    @StepScope
    @Bean
    public JdbcPagingItemReader<Person> reader(DataSource dataSource,
                                               @Value("#{stepExecutionContext[minValue]}") Integer minValue,
                                               @Value("#{stepExecutionContext[maxValue]}") Integer maxValue
                                               ) throws Exception {

        final SqlPagingQueryProviderFactoryBean sqlPagingQueryProviderFactoryBean
                = new SqlPagingQueryProviderFactoryBean();
        Map<String, Order> sortKeys = new HashMap();
        sortKeys.put("person_id", Order.ASCENDING);
        return new JdbcPagingItemReaderBuilder<Person>()
                .name("jdbcPersonItemReader")
                .dataSource(dataSource)
                .selectClause("select person_id, first_name, last_name")
                .fromClause("from person")
                .whereClause("where person_id >= " + minValue + " and person_id <=" + maxValue)
                .sortKeys(sortKeys)
                .pageSize(100)
                .rowMapper(new RowMapper<Person>() {
                    @Override
                    public Person mapRow(ResultSet resultSet, int i) throws SQLException {
                        Person person = new Person();
                        person.setId(resultSet.getLong("person_id"));
                        person.setFirstName(resultSet.getString("first_name"));
                        person.setLastName(resultSet.getString("last_name"));
                        return person;
                    }
                }).build();
    }

    /**
     * Converts name to upper case.
     * @return
     */
    @StepScope
    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }



    /**
     * Writes data to Person2 table.
     * @param dataSource
     * @return
     */
    @StepScope
    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO person2 (person_id, first_name, last_name) VALUES (:id, :firstName, :lastName)")
                .dataSource(dataSource)
                .build();
    }


    /**
     * This is the job that tests partitioning. This will read names from Person table, convert to upper case
     * and enter them into Person2 table.
     * @param listener
     * @param partitionStep
     * @return
     */
    @Bean
    @Profile("master")
    public Job transformUserJob(JobCompletionNotificationListener listener, Step partitionStep) {
        return jobBuilderFactory.get("transformUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(partitionStep)
                .build();
    }


    /**
     * Master partition step
     * @param dataSource
     * @param writer
     * @return
     * @throws Exception
     */
    @Bean
    public Step partitionStep(DataSource dataSource, JdbcBatchItemWriter<Person> writer) throws Exception {
        return stepBuilderFactory.get("partitionStep")
                .partitioner("slaveStep", partitioner(dataSource))
                .partitionHandler(remoteHandler(null))
                .build();
    }

    /**
     * Creates ExecutionContext for each step execution
     * @param dataSource
     * @return
     */
    @Bean
    public ColumnRangePartitioner partitioner(DataSource dataSource) {
        ColumnRangePartitioner partitioner = new ColumnRangePartitioner();
        partitioner.setColumn("person_id");
        partitioner.setTable("person");
        partitioner.setDataSource(dataSource);

        return partitioner;
    }

    /**
     * Executes each step execution for the partitioned step using local partitioning
     * @param writer
     * @param dataSource
     * @return
     * @throws Exception
     */
    @Bean
    public TaskExecutorPartitionHandler localHandler(JdbcBatchItemWriter<Person> writer, DataSource dataSource
    ) throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setGridSize(3);
        handler.setTaskExecutor(taskExecutor());
        handler.setStep(slaveStep(writer, dataSource));
        return handler;
    }

    @Bean
    public PartitionHandler remoteHandler(MessagingTemplate messagingTemplate) throws Exception {
        MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();
        partitionHandler.setStepName("slaveStep");
        partitionHandler.setGridSize(GRID_SIZE);
        partitionHandler.setMessagingOperations(messagingTemplate);
        partitionHandler.setPollInterval(5000);
        partitionHandler.setJobExplorer(this.jobExplorer);
        partitionHandler.afterPropertiesSet();
        partitionHandler.setReplyChannel((PollableChannel) applicationContext.getBean("outboundStaging"));
        return partitionHandler;
    }

    @Bean
    @Profile("slave")
    @ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundStaging")
    public StepExecutionRequestHandler stepExecutionRequestHandler() {
        StepExecutionRequestHandler stepExecutionRequestHandler =
                new StepExecutionRequestHandler();

        BeanFactoryStepLocator stepLocator = new BeanFactoryStepLocator();
        stepLocator.setBeanFactory(this.applicationContext);
        stepExecutionRequestHandler.setStepLocator(stepLocator);
        stepExecutionRequestHandler.setJobExplorer(this.jobExplorer);

        return stepExecutionRequestHandler;
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10));
        return pollerMetadata;
    }


    @Bean
    ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(10);
        threadPoolTaskExecutor.setMaxPoolSize(10);
        return threadPoolTaskExecutor;
    }

    /**
     * Slave processing step
     * @param writer
     * @param dataSource
     * @return
     * @throws Exception
     */
    @Bean
    public Step slaveStep(JdbcBatchItemWriter<Person> writer, DataSource dataSource
                          )
            throws Exception {
        return stepBuilderFactory.get("slaveStep").<Person, Person>chunk(10)
                .reader(reader(dataSource, null, null))
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


}
