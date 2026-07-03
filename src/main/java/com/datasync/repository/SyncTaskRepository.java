package com.datasync.repository;

import com.datasync.entity.SyncTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface SyncTaskRepository extends JpaRepository<SyncTask, Long> {
    List<SyncTask> findByEnabledTrue();
    List<SyncTask> findByLastExecStatus(String lastExecStatus);

    @Query(value = "SELECT id, task_name, sync_type, engine, deploy_mode, source_id, target_id, " +
           "source_name, target_name, status, cluster_id, description, enabled, cron_expression, " +
           "schedule_enabled, last_exec_status, last_exec_time, last_exec_duration, last_exec_rows, " +
           "last_exec_qps, incremental_where, schema_save_mode, data_save_mode, created_by, created_at, updated_at " +
           "FROM sync_task ORDER BY created_at DESC, id DESC",
           nativeQuery = true)
    List<Object[]> findTaskSummaries();

    @Query(value = "SELECT COALESCE(sync_type, 'unknown') AS sync_type, " +
           "COALESCE(last_exec_status, 'NONE') AS last_exec_status, COUNT(*) AS cnt " +
           "FROM sync_task GROUP BY COALESCE(sync_type, 'unknown'), COALESCE(last_exec_status, 'NONE')",
           nativeQuery = true)
    List<Object[]> getDashboardStats();

    @Query("SELECT t FROM SyncTask t WHERE " +
           "(:keyword IS NULL OR t.taskName LIKE %:keyword% OR t.description LIKE %:keyword%)")
    List<SyncTask> search(@Param("keyword") String keyword);
}
