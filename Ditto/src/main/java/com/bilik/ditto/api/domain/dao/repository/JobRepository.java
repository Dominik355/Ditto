package com.bilik.ditto.api.domain.dao.repository;

import com.bilik.ditto.api.domain.dao.entity.JobDao;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

public interface JobRepository extends CrudRepository<JobDao, Long> {

    @Query("SELECT * FROM job where job_id = :jobId")
    JobDao findByJobId(@Param("jobId") String jobId);

    @Modifying
    @Query("UPDATE job set error = :error where job_id = :jobId")
    void addError(@Param("jobId") String jobId, @Param("error") String error);

}
