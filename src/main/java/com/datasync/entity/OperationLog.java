package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "operation_log")
public class OperationLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 50)
    private String username;

    @Column(length = 20)
    private String action; // LOGIN / CREATE / UPDATE / DELETE / EXECUTE_SQL / TOGGLE / ...

    @Column(length = 200)
    private String target; // 操作对象，如 任务名、数据源名

    @Column(length = 500)
    private String detail; // 详细信息

    @Column(length = 50)
    private String ip;

    private Boolean success = true;

    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
