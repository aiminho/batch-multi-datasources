package br.com.robsonaraujo;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import br.com.robsonaraujo.model.Person;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("dataSource")
	private DataSource dataSource;
	
	@Autowired
	@Qualifier("postgresDs")
	private DataSource postgresDs;
	
	
	@Bean
	public BatchConfigurer batchConfigurer() {
		return new DefaultBatchConfigurer(dataSource);
	}
	
	@Bean
	@StepScope
	public Tasklet clearDatabaseTasklet(@Value("#{jobParameters['timestamp']}") String message, @Value("#{jobParameters['state']}") String state) {
		return (stepContribution, chunkContext) -> {	
			JdbcTemplate template = new JdbcTemplate(postgresDs);
			StringBuilder sql = new StringBuilder("delete from person where 1=1 ");
			if (state != null) {
				sql.append("and state = '" + state + "'");
			}
			
			template.execute(sql.toString());
			return RepeatStatus.FINISHED;
		};
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.tasklet(clearDatabaseTasklet(null,null))				
				.build();
	}
	
	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2")
				.<Person, Person>chunk(10)
				.reader(pagingItemReader(null))
				.writer(customItemWriter())
				.build();
	}
	
//	@Bean
//	private ItemWriter<Person> customItemWriter() {
//		return items -> items.forEach(System.out::println);
//	}
	
	@Bean
	public JdbcBatchItemWriter<Person> customItemWriter() {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(postgresDs);
		
		writer.setSql("INSERT INTO PERSON VALUES(:id, :firstName, :lastName, :email, :gender, :city, :state)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		//writer.afterPropertiesSet();
		
		return writer;
	}
	
	@Bean
	@StepScope // required to access jobParameters
	public JdbcPagingItemReader<Person> pagingItemReader(@Value("#{jobParameters['state']}") String state) {
		JdbcPagingItemReader<Person> reader = new JdbcPagingItemReader<>();
		
		reader.setDataSource(dataSource);
		reader.setFetchSize(10);
		reader.setRowMapper((rs, rowNum) -> 
			new Person(
				rs.getInt("id"),
				rs.getString("first_name"),
				rs.getString("last_name"),
				rs.getString("email"),
				rs.getString("gender"),
				rs.getString("state"),
				rs.getString("city")
			)
		);
		
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, first_name, last_name, email, gender, state, city");
		queryProvider.setFromClause("from person");
		
		if (state != null) {
			queryProvider.setWhereClause("where state = '" + state + "'");
		}
		
		Map<String, Order> sortKeys = new HashMap<>(1);
		sortKeys.put("id", Order.ASCENDING);
		
		queryProvider.setSortKeys(sortKeys);
		
		reader.setQueryProvider(queryProvider);
		
		return reader;
	}

//	private JdbcCursorItemReader<Person> cursorItemReader() {
//		JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();
//		reader.setDataSource(dataSource);
//		reader.setSql("select id, first_name, last_name, email, gender, state, city from person order by id");
//		reader.setRowMapper((rs, rowNum) -> 
//			new Person(
//				rs.getInt("id"),
//				rs.getString("first_name"),
//				rs.getString("last_name"),
//				rs.getString("email"),
//				rs.getString("gender"),
//				rs.getString("state"),
//				rs.getString("city")
//			)
//		);
//		return reader;
//	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("helloworldjob")
				.start(step1())
				.next(step2())
				.build();
	}
}
