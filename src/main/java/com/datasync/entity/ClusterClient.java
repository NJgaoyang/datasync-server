package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "cluster_client")
public class ClusterClient {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String clientName;
    private String host;
    private Integer port;
    private String status;
    private Double cpuUsage;
    private Double memoryUsage;
    private String description;

    /** SSH 远程执行: 端口(默认22) */
    private Integer sshPort = 22;
    /** SSH 用户名 */
    private String sshUser;
    /** SSH 密码(存储时 AES 加密) */
    private String sshPassword;
    /** Seatunnel 安装目录(远程) */
    private String seatunnelHome;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) status = "OFFLINE";
        if (cpuUsage == null) cpuUsage = 0.0;
        if (memoryUsage == null) memoryUsage = 0.0;
        if (sshPort == null) sshPort = 22;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
