package com.datasync.repository;

import com.datasync.entity.Datasource;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DatasourceRepository extends JpaRepository<Datasource, Long> {
    List<Datasource> findByType(String type);
}