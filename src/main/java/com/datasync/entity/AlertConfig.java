package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "alert_config")
public class AlertConfig {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 告警配置名称 */
    @Column(nullable = false, length = 100)
    private String name;

    /** 渠道类型: DINGTALK / FEISHU */
    @Column(nullable = false, length = 20)
    private String type;

    /** Webhook 地址 */
    @Column(name = "webhook_url", nullable = false, length = 500)
    private String webhookUrl;

    /** 签名密钥（钉钉加签/飞书签名校验，可选） */
    @Column(length = 200)
    private String secret;

    /** 钉钉机器人关键词（安全校验用，可选） */
    @Column(length = 100)
    private String keyword;

    /** 自定义告警模板，支持占位符 {taskName} {status} {message} {duration} {rows} {qps} {time} */
    @Column(columnDefinition = "TEXT")
    private String template;

    /** 是否启用 */
    @Column(nullable = false)
    private Boolean enabled = true;

    /** 触发事件类型: FAILURE / SUCCESS / ALL */
    @Column(name = "event_types", nullable = false, length = 50)
    private String eventTypes = "FAILURE";

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
