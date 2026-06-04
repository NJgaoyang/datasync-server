package com.datasync.repository;

import com.datasync.entity.TaskExecution;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

public interface TaskExecutionRepository extends JpaRepository<TaskExecution, Long> {
    List<TaskExecution> findByTaskIdOrderByStartedAtDesc(Long taskId);
    List<TaskExecution> findByTaskIdAndStatusOrderByStartedAtDesc(Long taskId, String status);
    List<TaskExecution> findByStatus(String status);

    /** 关联任务名搜索，分页（显式指定列序确保映射正确，支持日期+状态筛选） */
    @Query(value = "SELECT e.id, e.task_id, e.status, e.log_text, e.started_at, e.finished_at, " +
           "e.triggered_by, t.task_name, t.sync_type, t.created_by FROM task_execution e " +
           "LEFT JOIN sync_task t ON e.task_id = t.id " +
           "WHERE (:keyword IS NULL OR t.task_name LIKE CONCAT('%',:keyword,'%')) " +
           "AND (:startDate IS NULL OR e.started_at >= :startDate) " +
           "AND (:endDate IS NULL OR e.started_at <= :endDate) " +
           "AND (:status IS NULL OR e.status = :status) " +
           "ORDER BY e.started_at DESC",
           countQuery = "SELECT COUNT(*) FROM task_execution e LEFT JOIN sync_task t ON e.task_id = t.id " +
           "WHERE (:keyword IS NULL OR t.task_name LIKE CONCAT('%',:keyword,'%')) " +
           "AND (:startDate IS NULL OR e.started_at >= :startDate) " +
           "AND (:endDate IS NULL OR e.started_at <= :endDate) " +
           "AND (:status IS NULL OR e.status = :status)",
           nativeQuery = true)
    Page<Object[]> findAllWithTaskName(@Param("keyword") String keyword,
                                        @Param("startDate") String startDate,
                                        @Param("endDate") String endDate,
                                        @Param("status") String status,
                                        Pageable pageable);

    /** 当日各状态统计 */
    @Query(value = "SELECT status, COUNT(*) as cnt FROM task_execution WHERE DATE(started_at) = CURDATE() GROUP BY status", nativeQuery = true)
    List<Object[]> getTodayStats();

    /** 日期范围内按天+状态分组统计 */
    @Query(value = "SELECT DATE(started_at) as exec_date, status, COUNT(*) as cnt FROM task_execution WHERE started_at BETWEEN :start AND :end GROUP BY exec_date, status ORDER BY exec_date", nativeQuery = true)
    List<Object[]> getExecutionStatsByDate(@Param("start") String start, @Param("end") String end);

    /** 批量删除执行记录 */
    @Modifying
    @Transactional
    @Query("DELETE FROM TaskExecution e WHERE e.id IN :ids")
    int deleteByIdIn(@Param("ids") List<Long> ids);
}
