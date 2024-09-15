package com.bilik.ditto.api.domain.dao.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

@Data
@Table("counters")
public class CounterDao {

    @Id
    private long id;
    @Transient
    private JobDao job;
    private String name;
    private Long counterVal;

    public CounterDao(String name, Long counterVal) {
        this.name = name;
        this.counterVal = counterVal;
    }
}
