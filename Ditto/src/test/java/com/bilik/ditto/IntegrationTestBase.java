package com.bilik.ditto;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.test.context.ActiveProfiles;

import javax.sql.DataSource;

@Configuration
@Profile("test")
public class IntegrationTestBase {

    @Bean
    public DataSource dataSource() {
        System.out.println("creating datasource");

        return DataSourceBuilder.create()
                .driverClassName("org.h2.Driver")
                .url("jdbc:h2:mem:test;DATABASE_TO_LOWER=TRUE;DB_CLOSE_ON_EXIT=FALSE;MODE=MySQL")
                .username("sa")
                .password("")
                .build();
    }
}
