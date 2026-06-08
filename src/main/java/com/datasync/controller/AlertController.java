package com.datasync.controller;

import com.datasync.config.LoginInterceptor;
import com.datasync.entity.AlertConfig;
import com.datasync.service.AlertService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/alert")
public class AlertController {

    @Autowired
    private AlertService alertService;

    @Autowired(required = false)
    private HttpServletRequest request;

    /** 校验管理员 */
    private void requireAdmin() {
        String token = request.getHeader("X-Auth-Token");
        String role = LoginInterceptor.getRole(token);
        if (!"ADMIN".equals(role)) {
            throw new RuntimeException("仅管理员可执行此操作");
        }
    }

    /**
     * 获取告警配置列表
     */
    @GetMapping("/list")
    public List<AlertConfig> list() {
        return alertService.list();
    }

    /**
     * 新增告警配置
     */
    @PostMapping("/save")
    public AlertConfig save(@RequestBody AlertConfig cfg) {
        requireAdmin();
        return alertService.save(cfg);
    }

    /**
     * 更新告警配置
     */
    @PutMapping("/update")
    public AlertConfig update(@RequestBody AlertConfig cfg) {
        requireAdmin();
        return alertService.update(cfg);
    }

    /**
     * 删除告警配置
     */
    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        requireAdmin();
        alertService.delete(id);
    }

    /**
     * 启用/禁用切换
     */
    @PostMapping("/toggle/{id}")
    public AlertConfig toggle(@PathVariable Long id) {
        requireAdmin();
        return alertService.toggle(id);
    }

    /**
     * 测试告警配置（返回详细的成功/失败信息）
     */
    @PostMapping("/test/{id}")
    public Map<String, Object> test(@PathVariable Long id) {
        requireAdmin();
        AlertConfig cfg = alertService.list().stream()
                .filter(c -> c.getId().equals(id))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("告警配置不存在"));
        try {
            return alertService.testAlert(cfg);
        } catch (RuntimeException e) {
            Map<String, Object> errorResult = new java.util.LinkedHashMap<>();
            errorResult.put("success", false);
            errorResult.put("message", e.getMessage());
            return errorResult;
        }
    }
}
