package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "sync_task")
public class SyncTask {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String taskName;
    private String syncType;   // batch / realtime
    private String engine;     // ZETA / FLINK / SPARK
    private String deployMode; // client / cluster
    private Long sourceId;
    private Long targetId;
    private String sourceName;
    private String targetName;
    private String status;     // DRAFT / GENERATED / RUNNING
    private Long clusterId;    // 关联集群客户端
    private String description;

    /** 是否在线（启用调度） */
    private Boolean enabled = false;

    /** Cron 定时表达式 */
    private String cronExpression;

    /** 是否启用定时调度 */
    private Boolean scheduleEnabled = false;

    /** 上次执行状态: NONE / SUCCESS / FAILED / RUNNING */
    private String lastExecStatus;

    /** 上次执行时间 */
    private LocalDateTime lastExecTime;

    /** 上次执行耗时，如 "1分30秒" */
    private String lastExecDuration;

    /** 上次执行写入行数 */
    private Long lastExecRows;

    /** 上次执行QPS，如 "120.5/s" */
    private String lastExecQps;

    /** 增量WHERE条件（自定义自由输入，如 create_time >= DATE_SUB(NOW(),INTERVAL 1 DAY)），为空则全量 */
    @Column(columnDefinition = "TEXT")
    private String incrementalWhere;

    @Column(columnDefinition = "TEXT")
    private String seatunnelConfig;

    /** 创建人 */
    private String createdBy = "admin";

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (enabled == null) enabled = false;
        if (lastExecStatus == null) lastExecStatus = "NONE";
    }
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
