package com.bilik.ditto.api.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.sql.DataSource;

@Profile({ "!test" })
@Configuration
public class DbConfig {

    private static final Logger log = LoggerFactory.getLogger(DbConfig.class);

    @Value("${DB_USER_NAME}")
    private String user;

    @Value("${DB_PASSWORD}")
    private String password;

    @Value("${DB_URL}")
    private String url;

    @Value("${DB_PORT}")
    private String port;

    @Value("${DB_NAME}")
    private String name;

    @Bean
    public DataSource dataSource() {
        String formattedUrl = String.format("jdbc:mysql://%s:%s/%s", url, port, name);
        log.info("Creating DataSource Bean for url={}", formattedUrl);
        return DataSourceBuilder.create()
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .url(formattedUrl)
                .username(user)
                .password(password)
                .build();
    }

}
