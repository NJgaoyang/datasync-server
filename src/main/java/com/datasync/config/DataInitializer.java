package com.datasync.config;

import com.datasync.entity.SysUser;
import com.datasync.repository.SysUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

@Component
public class DataInitializer implements CommandLineRunner {
    @Autowired private SysUserRepository userRepository;
    private static final BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();

    @Override
    public void run(String... args) {
        // 双重加密：先 SHA-256（前端相同），再 BCrypt（后端存储）
        String sha256 = sha256Hex("admin123");
        String encodedPwd = encoder.encode(sha256);

        Optional<SysUser> optAdmin = userRepository.findByUsername("admin");
        if (optAdmin.isPresent()) {
            // 已存在则不覆盖密码
            SysUser admin = optAdmin.get();
            admin.setEnabled(true);
            userRepository.save(admin);
        } else {
            SysUser admin = new SysUser();
            admin.setUsername("admin");
            admin.setPassword(encodedPwd);
            admin.setRealName("管理员");
            admin.setRole("ADMIN");
            admin.setEnabled(true);
            userRepository.save(admin);
            System.out.println("[Init] 管理员用户已创建: admin / admin123");
        }
    }

    private String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
