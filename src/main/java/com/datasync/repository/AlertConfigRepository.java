package com.datasync.repository;

import com.datasync.entity.AlertConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlertConfigRepository extends JpaRepository<AlertConfig, Long> {

    /** 查询所有启用的告警配置 */
    List<AlertConfig> findByEnabledTrue();
}
