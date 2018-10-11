package example.batch;

import example.domain.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    ExecutionContext executionContext;

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        executionContext = stepExecution.getExecutionContext();
    }

    @Override
    public Person process(final Person person) throws Exception {
        final String firstName = person.getFirstName().toUpperCase();
        final String lastName = person.getLastName().toUpperCase();

        Person transformedPerson = new Person(firstName, lastName);
        transformedPerson.setId(person.getId());

        Thread.sleep(1); // Artificial delay to increase processing time

        log.info("Converting (" + person + ") into (" + transformedPerson + ")");


        log.info( String.format("Thread name = %s, Execution context minValue = %s, maxValue = %s, PersonId=%s",
                 Thread.currentThread().getName()
                , executionContext.getInt("minValue")
                , executionContext.getInt("maxValue")
        ,person.getId()));

        return transformedPerson;
    }

}
