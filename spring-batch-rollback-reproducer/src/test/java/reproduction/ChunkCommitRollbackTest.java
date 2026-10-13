/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reproduction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class ChunkCommitRollbackTest {
  @Container
  private static final PostgreSQLContainer DATABASE =
      new PostgreSQLContainer("postgres:17.11-bookworm");

  enum Engine {
    COMPATIBILITY,
    NATIVE
  }

  @ParameterizedTest(name = "{0}: confirmed rollback remains restartable")
  @EnumSource(Engine.class)
  @SuppressWarnings("removal")
  void confirmedPreCommitRollbackLeavesRestartableFailedJob(Engine engine) throws Exception {
    var admin =
        new DriverManagerDataSource(
            DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
    String schema = engine.name().toLowerCase(java.util.Locale.ROOT);
    new JdbcTemplate(admin).execute("create schema " + schema);
    var source =
        new DriverManagerDataSource(
            DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
    var connectionProperties = new Properties();
    connectionProperties.setProperty("currentSchema", schema);
    source.setConnectionProperties(connectionProperties);
    new ResourceDatabasePopulator(
            new ClassPathResource("org/springframework/batch/core/schema-postgresql.sql"))
        .execute(source);
    var transactionManager = new DataSourceTransactionManager(source);
    var factory = new JdbcJobRepositoryFactoryBean();
    factory.setDataSource(source);
    factory.setTransactionManager(transactionManager);
    // Use Batch's default serializer: no Jackson or application configuration.
    factory.afterPropertiesSet();
    var repository = factory.getObject();
    var jdbc = new JdbcTemplate(source);
    jdbc.execute("create table items (id bigint primary key, processed boolean not null)");
    jdbc.update("insert into items values (1, false), (2, false), (3, false)");

    ItemReader<Long> reader =
        () -> {
          var ids =
              jdbc.queryForList(
                  "select id from items where not processed order by id limit 1", Long.class);
          return ids.isEmpty() ? null : ids.get(0);
        };
    var attempts = new AtomicInteger();
    var failedCompletion = new AtomicInteger(-1);
    ItemWriter<Long> writer =
        items -> {
          for (Long id : items) {
            jdbc.update("update items set processed = true where id = ?", id);
          }
          if (attempts.incrementAndGet() == 2) {
            TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                  @Override
                  public void beforeCommit(boolean readOnly) {
                    throw new IllegalStateException("Deliberate pre-commit failure");
                  }

                  @Override
                  public void afterCompletion(int status) {
                    failedCompletion.set(status);
                  }
                });
          }
        };

    Step step =
        engine == Engine.NATIVE
            ? new StepBuilder("step", repository)
                .<Long, Long>chunk(1)
                .transactionManager(transactionManager)
                .reader(reader)
                .writer(writer)
                .build()
            : new StepBuilder("step", repository)
                .<Long, Long>chunk(1, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    var job = new JobBuilder("job", repository).start(step).build();
    var registry = new MapJobRegistry();
    registry.register(job);
    var operator = new TaskExecutorJobOperator();
    operator.setJobRepository(repository);
    operator.setJobRegistry(registry);
    operator.setTaskExecutor(new SyncTaskExecutor());
    operator.afterPropertiesSet();

    var failed = operator.start(job, new JobParameters());
    failed
        .getStepExecutions()
        .forEach(
            execution ->
                execution
                    .getFailureExceptions()
                    .forEach(failure -> System.out.println(engine + ": " + failure)));

    assertEquals(TransactionSynchronization.STATUS_ROLLED_BACK, failedCompletion.get());
    assertEquals(
        List.of(1L),
        jdbc.queryForList("select id from items where processed order by id", Long.class));
    assertEquals(BatchStatus.FAILED, failed.getStatus());

    var restarted = operator.restart(repository.getJobExecution(failed.getId()));

    assertEquals(BatchStatus.COMPLETED, restarted.getStatus());
    assertEquals(failed.getJobInstance(), restarted.getJobInstance());
    assertEquals(3L, jdbc.queryForObject("select count(*) from items where processed", Long.class));
  }
}
