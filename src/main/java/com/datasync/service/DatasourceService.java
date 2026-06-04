package com.datasync.service;

import com.datasync.entity.Datasource;
import com.datasync.repository.DatasourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;

@Service
public class DatasourceService {
    @Autowired
    private DatasourceRepository datasourceRepository;

    public List<Datasource> list() {
        return datasourceRepository.findAll();
    }

    public Datasource save(Datasource ds) {
        return datasourceRepository.save(ds);
    }

    public Datasource update(Datasource ds) {
        if (ds.getId() == null) {
            throw new RuntimeException("ID不能为空");
        }
        return datasourceRepository.save(ds);
    }

    public void delete(Long id) {
        datasourceRepository.deleteById(id);
    }

    public Datasource getById(Long id) {
        return datasourceRepository.findById(id).orElse(null);
    }

    public boolean testConnection(Datasource ds) {
        String url;
        if ("mysql".equalsIgnoreCase(ds.getType())) {
            url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                    ds.getHost(), ds.getPort(),
                    ds.getDatabaseName() == null ? "" : ds.getDatabaseName());
        } else if ("starrocks".equalsIgnoreCase(ds.getType())) {
            url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                    ds.getHost(), ds.getPort(),
                    ds.getDatabaseName() == null ? "" : ds.getDatabaseName());
        } else {
            return false;
        }
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword())) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}