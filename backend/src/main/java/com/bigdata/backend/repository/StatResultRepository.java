package com.bigdata.backend.repository;

import com.bigdata.backend.entity.StatResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StatResultRepository extends JpaRepository<StatResult, Long> {
}
