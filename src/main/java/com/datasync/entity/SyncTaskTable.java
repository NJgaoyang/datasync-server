package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "sync_task_table")
public class SyncTaskTable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long taskId;
    private String sourceDatabase;
    private String sourceTable;
    private String targetDatabase;
    private String targetTable;
    private String partitionColumn;
    private String syncType;
    @Column(columnDefinition = "TEXT")
    private String srDdl;
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}