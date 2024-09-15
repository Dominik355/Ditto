package com.bilik.ditto.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class TableSeeder implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TableSeeder.class);

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public TableSeeder(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Creating tables");

        jdbcTemplate.execute("""
                        CREATE TABLE IF NOT EXISTS job (
                            id BIGINT NOT NULL AUTO_INCREMENT,
                            job_id VARCHAR(255) NOT NULL,
                            parallelism INT,
                            error TEXT,
                            queue_size INT,
                            queued BOOLEAN,
                            source_type VARCHAR(128) NOT NULL,
                            sink_type VARCHAR(128) NOT NULL,
                            source_data_type VARCHAR(255) NOT NULL,
                            sink_data_type VARCHAR(255) NOT NULL,
                            special_converter_name VARCHAR(128),
                            special_converter_subtype VARCHAR(128),
                            special_converter_args VARCHAR(512),
                            queue_time TIMESTAMP,
                            creation_time TIMESTAMP,
                            start_time TIMESTAMP,
                            running_time TIMESTAMP,
                            finish_time TIMESTAMP,
                            PRIMARY KEY (id)
                        );
                   """);

        jdbcTemplate.execute("""
                         CREATE TABLE IF NOT EXISTS worker_event (
                        	id BIGINT NOT NULL AUTO_INCREMENT,
                            job BIGINT,
                            worker_type VARCHAR(64),
                            worker_state VARCHAR(64),
                            thread_num INT,
                            PRIMARY KEY (id),
                            CONSTRAINT FOREIGN KEY (job) REFERENCES job(id)
                        );
                """);

        jdbcTemplate.execute("""
                         CREATE TABLE IF NOT EXISTS counters (
                        	id BIGINT NOT NULL AUTO_INCREMENT,
                            job BIGINT,
                            name VARCHAR(255),
                            counter_val BIGINT,
                            PRIMARY KEY (id),
                            CONSTRAINT FOREIGN KEY (job) REFERENCES job(id)
                        );
                """);

        jdbcTemplate.execute("""
                CREATE SEQUENCE IF NOT EXISTS ditto_seq START WITH 1 INCREMENT BY 1;
                """);

        log.info("Tables created");
    }

}
