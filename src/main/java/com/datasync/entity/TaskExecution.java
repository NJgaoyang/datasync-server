package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "task_execution")
public class TaskExecution {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long taskId;

    /** PENDING / RUNNING / SUCCESS / FAILED */
    private String status;

    @Column(columnDefinition = "MEDIUMTEXT")
    private String logText;

    /** 触发人 */
    private String triggeredBy = "admin";

    private LocalDateTime startedAt;
    private LocalDateTime finishedAt;

    @PrePersist
    protected void onCreate() {
        startedAt = LocalDateTime.now();
    }
}
