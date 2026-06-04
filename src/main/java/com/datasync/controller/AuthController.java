package com.datasync.controller;

import com.datasync.config.LoginInterceptor;
import com.datasync.entity.SysUser;
import com.datasync.repository.SysUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/auth")
public class AuthController {
    @Autowired private SysUserRepository userRepository;
    private static final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody Map<String, Object> params) {
        String username = (String) params.get("username");
        String password = (String) params.get("password");
        Map<String, Object> result = new HashMap<>();

        Optional<SysUser> opt = userRepository.findByUsername(username);
        if (!opt.isPresent()) {
            result.put("success", false);
            result.put("message", "用户名或密码错误");
            return result;
        }
        SysUser user = opt.get();
        if (!Boolean.TRUE.equals(user.getEnabled())) {
            result.put("success", false);
            result.put("message", "用户已被禁用");
            return result;
        }
        // 前端传 SHA-256，后端 BCrypt 比较
        if (!encoder.matches(password, user.getPassword())) {
            result.put("success", false);
            result.put("message", "用户名或密码错误");
            return result;
        }

        // 生成 session token
        String token = LoginInterceptor.createSession(username, user.getRole());
        result.put("success", true);
        result.put("token", token);
        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put("id", user.getId());
        userInfo.put("username", user.getUsername());
        userInfo.put("realName", user.getRealName() != null ? user.getRealName() : "");
        userInfo.put("role", user.getRole());
        result.put("user", userInfo);
        return result;
    }

    @PostMapping("/logout")
    public Map<String, Object> logout(@RequestHeader("X-Auth-Token") String token) {
        LoginInterceptor.removeSession(token);
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        return result;
    }

    @GetMapping("/check")
    public Map<String, Object> check(@RequestHeader("X-Auth-Token") String token) {
        String username = LoginInterceptor.getUsername(token);
        Map<String, Object> result = new HashMap<>();
        result.put("success", username != null);
        result.put("username", username != null ? username : "");
        return result;
    }
}
