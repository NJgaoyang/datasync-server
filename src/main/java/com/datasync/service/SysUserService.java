package com.datasync.service;

import com.datasync.entity.*;
import com.datasync.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class SysUserService {
    @Autowired private SysUserRepository userRepository;
    @Autowired private UserPermissionRepository permissionRepository;
    private static final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    public List<SysUser> listAll() {
        return userRepository.findAll();
    }

    public SysUser getById(Long id) {
        return userRepository.findById(id).orElseThrow(() -> new RuntimeException("用户不存在"));
    }

    @Transactional
    public SysUser create(SysUser user) {
        if (userRepository.existsByUsername(user.getUsername())) {
            throw new RuntimeException("用户名已存在");
        }
        // 双重加密：输入已是 SHA-256，再用 BCrypt 加密存储
        user.setPassword(encoder.encode(user.getPassword()));
        return userRepository.save(user);
    }

    @Transactional
    public SysUser update(SysUser user) {
        SysUser existing = getById(user.getId());
        existing.setRealName(user.getRealName());
        existing.setRole(user.getRole());
        existing.setEnabled(user.getEnabled());
        if (user.getPassword() != null && !user.getPassword().trim().isEmpty()) {
            existing.setPassword(encoder.encode(user.getPassword()));
        }
        return userRepository.save(existing);
    }

    @Transactional
    public void delete(Long id) {
        permissionRepository.findByUserId(id).forEach(p -> permissionRepository.delete(p));
        userRepository.deleteById(id);
    }

    // ===== 权限 =====
    public List<UserPermission> getPermissions(Long userId) {
        return permissionRepository.findByUserId(userId);
    }

    @Transactional
    public UserPermission grantPermission(UserPermission perm) {
        UserPermission existing = permissionRepository.findByUserIdAndDatasourceId(perm.getUserId(), perm.getDatasourceId());
        if (existing != null) {
            existing.setCanExecuteSql(perm.getCanExecuteSql());
            return permissionRepository.save(existing);
        }
        return permissionRepository.save(perm);
    }

    @Transactional
    public void revokePermission(Long userId, Long datasourceId) {
        permissionRepository.deleteByUserIdAndDatasourceId(userId, datasourceId);
    }
}
