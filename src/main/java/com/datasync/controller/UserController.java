package com.datasync.controller;

import com.datasync.entity.*;
import com.datasync.service.SysUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/user")
public class UserController {
    @Autowired private SysUserService userService;

    @GetMapping("/list")
    public List<SysUser> list() {
        List<SysUser> list = userService.listAll();
        list.forEach(u -> u.setPassword(null));
        return list;
    }

    @GetMapping("/{id}")
    public SysUser getById(@PathVariable Long id) {
        SysUser u = userService.getById(id);
        u.setPassword(null);
        return u;
    }

    @PostMapping("/save")
    public SysUser create(@RequestBody SysUser user) {
        return userService.create(user);
    }

    @PutMapping("/update")
    public SysUser update(@RequestBody SysUser user) {
        return userService.update(user);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        userService.delete(id);
    }

    @GetMapping("/{userId}/permissions")
    public List<UserPermission> getPermissions(@PathVariable Long userId) {
        return userService.getPermissions(userId);
    }

    @PostMapping("/grant")
    public UserPermission grantPermission(@RequestBody UserPermission perm) {
        return userService.grantPermission(perm);
    }

    @PostMapping("/revoke")
    public void revokePermission(@RequestBody Map<String, Object> params) {
        Long userId = Long.valueOf(params.get("userId").toString());
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        userService.revokePermission(userId, datasourceId);
    }
}
