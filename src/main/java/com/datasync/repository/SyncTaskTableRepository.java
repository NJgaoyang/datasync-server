package com.datasync.repository;

import com.datasync.entity.SyncTaskTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import java.util.List;

public interface SyncTaskTableRepository extends JpaRepository<SyncTaskTable, Long> {
    List<SyncTaskTable> findByTaskId(Long taskId);
    List<SyncTaskTable> findByTaskIdOrderByIdAsc(Long taskId);
    void deleteByTaskId(Long taskId);

    @Query(value = "SELECT tt.id, tt.task_id, t.task_name, t.sync_type, t.enabled, t.last_exec_status, " +
            "tt.source_database, tt.source_table, tt.target_database, tt.target_table, tt.created_at " +
            "FROM sync_task_table tt JOIN sync_task t ON tt.task_id = t.id " +
            "WHERE t.enabled = 1 OR t.last_exec_status IN ('SUCCESS', 'RUNNING') " +
            "ORDER BY t.task_name ASC, tt.source_database ASC, tt.source_table ASC, tt.id ASC",
            nativeQuery = true)
    List<Object[]> findSyncedTableSummaries();
}
