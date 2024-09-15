package com.bilik.ditto.api;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;

@Configuration
@EnableJdbcRepositories("com.bilik.ditto.api.domain.dao.repository")
public class DittoConfiguration {
}
