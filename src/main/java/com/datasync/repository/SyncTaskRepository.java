package com.datasync.repository;

import com.datasync.entity.SyncTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface SyncTaskRepository extends JpaRepository<SyncTask, Long> {
    List<SyncTask> findByEnabledTrue();
    List<SyncTask> findByLastExecStatus(String lastExecStatus);

    @Query("SELECT t FROM SyncTask t WHERE " +
           "(:keyword IS NULL OR t.taskName LIKE %:keyword% OR t.description LIKE %:keyword%)")
    List<SyncTask> search(@Param("keyword") String keyword);
}
