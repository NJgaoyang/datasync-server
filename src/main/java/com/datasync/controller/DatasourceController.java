package com.datasync.controller;

import com.datasync.config.LoginInterceptor;
import com.datasync.entity.Datasource;
import com.datasync.service.DatasourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

@RestController
@RequestMapping("/api/datasource")
public class DatasourceController {
    @Autowired
    private DatasourceService datasourceService;
    @Autowired(required = false)
    private HttpServletRequest request;

    /** 校验是否为管理员，非管理员抛出异常 */
    private void requireAdmin() {
        String token = request.getHeader("X-Auth-Token");
        String role = LoginInterceptor.getRole(token);
        if (!"ADMIN".equals(role)) {
            throw new RuntimeException("仅管理员可执行此操作");
        }
    }

    @GetMapping("/list")
    public List<Datasource> list() {
        return datasourceService.list();
    }

    @PostMapping("/save")
    public Datasource save(@RequestBody Datasource ds) {
        requireAdmin();
        return datasourceService.save(ds);
    }

    @PutMapping("/update")
    public Datasource update(@RequestBody Datasource ds) {
        requireAdmin();
        return datasourceService.update(ds);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        requireAdmin();
        datasourceService.delete(id);
    }

    @PostMapping("/test")
    public boolean testConnection(@RequestBody Datasource ds) {
        return datasourceService.testConnection(ds);
    }
}