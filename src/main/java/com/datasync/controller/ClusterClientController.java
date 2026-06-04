package com.datasync.controller;

import com.datasync.entity.ClusterClient;
import com.datasync.service.ClusterClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cluster")
public class ClusterClientController {
    @Autowired
    private ClusterClientService service;

    @GetMapping("/list")
    public List<ClusterClient> list() {
        return service.list();
    }

    @PostMapping("/save")
    public ClusterClient save(@RequestBody ClusterClient client) {
        return service.save(client);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        service.delete(id);
    }

    /** 检查单个客户端健康状态（SSH 连接 + SeaTunnel 可用性） */
    @PostMapping("/{id}/health")
    public Map<String, Object> checkHealth(@PathVariable Long id) {
        return service.checkHealth(id);
    }

    /** 批量检查所有客户端健康状态 */
    @PostMapping("/health-check-all")
    public Map<String, Object> checkAllHealth() {
        service.checkAllHealth();
        Map<String, Object> result = new java.util.HashMap<>();
        result.put("ok", true);
        return result;
    }
}
