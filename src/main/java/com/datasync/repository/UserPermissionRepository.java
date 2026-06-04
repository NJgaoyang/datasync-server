package com.datasync.repository;

import com.datasync.entity.UserPermission;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface UserPermissionRepository extends JpaRepository<UserPermission, Long> {
    List<UserPermission> findByUserId(Long userId);
    UserPermission findByUserIdAndDatasourceId(Long userId, Long datasourceId);
    void deleteByUserIdAndDatasourceId(Long userId, Long datasourceId);
}
