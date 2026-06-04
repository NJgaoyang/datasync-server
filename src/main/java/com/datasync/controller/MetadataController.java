package com.datasync.controller;

import com.datasync.service.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {
    @Autowired
    private MetadataService metadataService;

    @GetMapping("/databases")
    public List<String> getDatabases(@RequestParam Long datasourceId) {
        return metadataService.getDatabases(datasourceId);
    }

    @GetMapping("/tables")
    public List<Map<String, String>> getTables(@RequestParam Long datasourceId, @RequestParam String database) {
        return metadataService.getTables(datasourceId, database);
    }

    @GetMapping("/columns")
    public List<Map<String, String>> getColumns(@RequestParam Long datasourceId,
                                                @RequestParam String database,
                                                @RequestParam String tableName) {
        return metadataService.getColumns(datasourceId, database, tableName);
    }

    @PostMapping("/generate-ddl")
    public Map<String, String> generateSingleDdl(@RequestBody Map<String, Object> params) {
        Long sourceId = Long.valueOf(params.get("sourceId").toString());
        String sourceDatabase = (String) params.get("sourceDatabase");
        String sourceTable = (String) params.get("sourceTable");
        String targetDatabase = (String) params.get("targetDatabase");
        String targetTable = (String) params.get("targetTable");
        String ddl = metadataService.generateSingleDdl(sourceId, sourceDatabase, sourceTable, targetDatabase, targetTable);
        Map<String, String> result = new HashMap<>();
        result.put("ddl", ddl);
        return result;
    }

    @PostMapping("/generate-batch-ddl")
    public Map<String, String> generateBatchDdl(@RequestBody Map<String, Object> params) {
        Long sourceId = Long.valueOf(params.get("sourceId").toString());
        String sourceDatabase = (String) params.get("sourceDatabase");
        List<String> tables = (List<String>) params.get("tables");
        String targetDatabase = (String) params.get("targetDatabase");
        String ddl = metadataService.generateBatchDdl(sourceId, sourceDatabase, tables, targetDatabase);
        Map<String, String> result = new HashMap<>();
        result.put("ddl", ddl);
        return result;
    }

    @PostMapping("/refresh")
    public void refreshMetadata(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        metadataService.refreshMetadata(datasourceId);
    }

    @GetMapping("/preview-data")
    public Map<String, Object> previewData(@RequestParam Long datasourceId,
                                           @RequestParam String database,
                                           @RequestParam String tableName) {
        return metadataService.previewData(datasourceId, database, tableName);
    }

    /** 执行 SQL（仅限 StarRocks） */
    @PostMapping("/execute-sql")
    public Map<String, Object> executeSql(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        String database = (String) params.get("database");
        String sql = (String) params.get("sql");
        return metadataService.executeSql(datasourceId, database, sql);
    }

    /** 删除表（仅限 StarRocks） */
    @PostMapping("/drop-table")
    public void dropTable(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        String database = (String) params.get("database");
        String tableName = (String) params.get("tableName");
        metadataService.dropTable(datasourceId, database, tableName);
    }

    /** 查询表数据（全部 + limit） */
    @GetMapping("/table-data")
    public Map<String, Object> queryTableData(@RequestParam Long datasourceId,
                                              @RequestParam String database,
                                              @RequestParam String tableName,
                                              @RequestParam(defaultValue = "100") int limit) {
        return metadataService.queryTableData(datasourceId, database, tableName, limit);
    }

    /** 新增行（仅限 StarRocks） */
    @PostMapping("/insert-row")
    public void insertRow(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        String database = (String) params.get("database");
        String tableName = (String) params.get("tableName");
        @SuppressWarnings("unchecked")
        Map<String, Object> values = (Map<String, Object>) params.get("values");
        metadataService.insertRow(datasourceId, database, tableName, values);
    }

    /** 更新行（仅限 StarRocks） */
    @PostMapping("/update-row")
    public void updateRow(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        String database = (String) params.get("database");
        String tableName = (String) params.get("tableName");
        String pkColumn = (String) params.get("pkColumn");
        String pkValue = String.valueOf(params.get("pkValue"));
        @SuppressWarnings("unchecked")
        Map<String, Object> values = (Map<String, Object>) params.get("values");
        metadataService.updateRow(datasourceId, database, tableName, pkColumn, pkValue, values);
    }

    /** 删除行（仅限 StarRocks） */
    @PostMapping("/delete-row")
    public void deleteRow(@RequestBody Map<String, Object> params) {
        Long datasourceId = Long.valueOf(params.get("datasourceId").toString());
        String database = (String) params.get("database");
        String tableName = (String) params.get("tableName");
        String pkColumn = (String) params.get("pkColumn");
        String pkValue = String.valueOf(params.get("pkValue"));
        metadataService.deleteRow(datasourceId, database, tableName, pkColumn, pkValue);
    }
}