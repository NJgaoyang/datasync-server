package com.datasync.entity;

import lombok.Data;
import javax.persistence.*;

@Data
@Entity
@Table(name = "user_permission")
public class UserPermission {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private Long datasourceId;

    /** 是否允许执行 SQL（仅 StarRocks） */
    private Boolean canExecuteSql = false;
}
