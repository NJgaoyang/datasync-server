package com.datasync.repository;

import com.datasync.entity.SyncTaskTable;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface SyncTaskTableRepository extends JpaRepository<SyncTaskTable, Long> {
    List<SyncTaskTable> findByTaskId(Long taskId);
    void deleteByTaskId(Long taskId);
}