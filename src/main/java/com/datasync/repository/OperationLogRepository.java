package com.datasync.repository;

import com.datasync.entity.OperationLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OperationLogRepository extends JpaRepository<OperationLog, Long> {

    @Query(value = "SELECT * FROM operation_log WHERE " +
           "(:keyword IS NULL OR username LIKE CONCAT('%',:keyword,'%') OR target LIKE CONCAT('%',:keyword,'%')) " +
           "AND (:username IS NULL OR username = :username) " +
           "AND (:startDate IS NULL OR created_at >= :startDate) " +
           "AND (:endDate IS NULL OR created_at <= CONCAT(:endDate,' 23:59:59')) " +
           "ORDER BY created_at DESC",
           countQuery = "SELECT COUNT(*) FROM operation_log WHERE " +
           "(:keyword IS NULL OR username LIKE CONCAT('%',:keyword,'%') OR target LIKE CONCAT('%',:keyword,'%')) " +
           "AND (:username IS NULL OR username = :username) " +
           "AND (:startDate IS NULL OR created_at >= :startDate) " +
           "AND (:endDate IS NULL OR created_at <= CONCAT(:endDate,' 23:59:59'))",
           nativeQuery = true)
    Page<OperationLog> search(@Param("keyword") String keyword,
                               @Param("startDate") String startDate,
                               @Param("endDate") String endDate,
                               @Param("username") String username,
                               Pageable pageable);
}
