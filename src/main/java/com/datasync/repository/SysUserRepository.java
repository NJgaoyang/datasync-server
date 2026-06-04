package com.datasync.repository;

import com.datasync.entity.SysUser;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface SysUserRepository extends JpaRepository<SysUser, Long> {
    Optional<SysUser> findByUsername(String username);
    boolean existsByUsername(String username);
}
