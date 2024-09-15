package com.bilik.ditto.api.domain.dao.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;

@Repository
public class JobIdSequencer {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public JobIdSequencer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public long nextVal() {
        return jdbcTemplate.query("SELECT NEXTVAL(ditto_seq);",
                rs -> {
                    if (rs.next()) {
                        return rs.getLong(1);
                    } else {
                        throw new SQLException("Unable to retrieve value from sequence ditto_seq.");
                    }
                });
    }

}
