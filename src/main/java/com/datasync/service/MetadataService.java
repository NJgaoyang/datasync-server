package com.datasync.service;

import com.datasync.entity.Datasource;
import com.datasync.util.TypeMappingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class MetadataService {
    @Autowired
    private DatasourceService datasourceService;

    /**
     * 获取MySQL数据源的所有数据库列表
     */
    public List<String> getDatabases(Long datasourceId) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) {
            throw new RuntimeException("数据源不存在");
        }
        // 注意：StarRocks 和 MySQL 都使用 MySQL 协议，JDBC 连接方式相同
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true",
                ds.getHost(), ds.getPort());
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
            List<String> databases = new ArrayList<>();
            while (rs.next()) {
                databases.add(rs.getString(1));
            }
            // 过滤系统库
            return databases.stream()
                    .filter(db -> !db.equalsIgnoreCase("information_schema")
                            && !db.equalsIgnoreCase("performance_schema")
                            && !db.equalsIgnoreCase("mysql")
                            && !db.equalsIgnoreCase("sys")
                            && !db.equalsIgnoreCase("_statistics_")) // 过滤 StarRocks 内部库
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace(); // 方便排查
            throw new RuntimeException("获取数据库列表失败: " + e.getMessage(), e);
        }
    }
    /**
     * 获取指定数据库下的所有表
     */
    public List<Map<String, String>> getTables(Long datasourceId, String database) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLE STATUS")) {
            List<Map<String, String>> tables = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> tableInfo = new HashMap<>();
                tableInfo.put("tableName", rs.getString("Name"));
                tableInfo.put("tableComment", rs.getString("Comment"));
                tables.add(tableInfo);
            }
            return tables;
        } catch (Exception e) {
            throw new RuntimeException("获取表列表失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取指定表的列信息，并附带 StarRocks 类型映射
     */
    public List<Map<String, String>> getColumns(Long datasourceId, String database, String tableName) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW FULL COLUMNS FROM " + quoteIdentifier(tableName))) {
            List<Map<String, String>> columns = new ArrayList<>();
            while (rs.next()) {
                Map<String, String> col = new HashMap<>();
                String colName = rs.getString("Field");
                String colType = rs.getString("Type");
                String nullable = rs.getString("Null");
                String key = rs.getString("Key");
                String comment = rs.getString("Comment");

                col.put("columnName", colName);
                col.put("columnType", colType);
                col.put("srType", TypeMappingUtil.mysqlToStarRocks(colType));
                col.put("isNullable", "YES".equalsIgnoreCase(nullable) ? "YES" : "NO");
                col.put("columnKey", key);
                col.put("columnComment", comment == null ? "" : comment);
                columns.add(col);
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("获取列信息失败: " + e.getMessage(), e);
        }
    }

    /**
     * 生成单表的 StarRocks DDL（基于实际列信息）
     */
    public String generateSingleDdl(Long sourceId, String sourceDatabase, String sourceTable,
                                    String targetDatabase, String targetTable) {
        List<Map<String, String>> columns = getColumns(sourceId, sourceDatabase, sourceTable);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS `").append(targetDatabase).append("`.`").append(targetTable).append("` (\n");
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Map<String, String> col = columns.get(i);
            String colName = col.get("columnName");
            String srType = col.get("srType");
            String nullable = col.get("isNullable");
            String comment = col.get("columnComment");
            String key = col.get("columnKey");

            ddl.append("  `").append(colName).append("` ").append(srType);
            // 只主键 NOT NULL，普通列允许 NULL（兼容源数据质量）
            if ("PRI".equalsIgnoreCase(key)) {
                ddl.append(" NOT NULL");
            }
            if (comment != null && !comment.isEmpty()) {
                ddl.append(" COMMENT '").append(escapeComment(comment)).append("'");
            }
            if (i < columns.size() - 1) {
                ddl.append(",\n");
            } else {
                ddl.append("\n");
            }
            if ("PRI".equalsIgnoreCase(key)) {
                primaryKeys.add(colName);
            }
        }
        if (!primaryKeys.isEmpty()) {
            ddl.append(") ENGINE=OLAP\n");
            ddl.append("PRIMARY KEY(");
            ddl.append(String.join(", ", primaryKeys.stream().map(pk -> "`" + pk + "`").collect(Collectors.toList())));
            ddl.append(")\n");
        } else {
            ddl.append(") ENGINE=OLAP\n");
        }
        ddl.append("DISTRIBUTED BY HASH(");
        if (!primaryKeys.isEmpty()) {
            ddl.append("`").append(primaryKeys.get(0)).append("`");
        } else {
            // 无主键时，默认按第一列哈希
            ddl.append("`").append(columns.get(0).get("columnName")).append("`");
        }
        ddl.append(") BUCKETS 3\n");
        ddl.append("PROPERTIES (\n");
        ddl.append("  \"replication_num\" = \"3\"\n");
        ddl.append(");");
        return ddl.toString();
    }

    /**
     * 批量生成 DDL（多个表用分号分隔）- 一次连接复用
     */
    public String generateBatchDdl(Long sourceId, String sourceDatabase, List<String> tables, String targetDatabase) {
        Datasource ds = datasourceService.getById(sourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), sourceDatabase);
        StringBuilder allDdl = new StringBuilder();
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword())) {
            for (String table : tables) {
                try {
                    List<Map<String, String>> columns = getColumnsFromConnection(conn, table);
                    String ddl = generateDdlFromColumns(columns, targetDatabase, table);
                    allDdl.append(ddl).append("\n\n");
                } catch (Exception e) {
                    allDdl.append("-- 表 ").append(table).append(" DDL 生成失败: ").append(e.getMessage()).append("\n\n");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("连接数据源失败: " + e.getMessage(), e);
        }
        return allDdl.toString();
    }

    /**
     * 从已有连接获取列信息（避免重复连接）
     */
    public List<Map<String, String>> getColumnsFromConnection(Connection conn, String tableName) throws Exception {
        List<Map<String, String>> columns = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW FULL COLUMNS FROM " + quoteIdentifier(tableName))) {
            while (rs.next()) {
                Map<String, String> col = new HashMap<>();
                col.put("columnName", rs.getString("Field"));
                col.put("columnType", rs.getString("Type"));
                col.put("srType", TypeMappingUtil.mysqlToStarRocks(rs.getString("Type")));
                col.put("isNullable", "YES".equalsIgnoreCase(rs.getString("Null")) ? "YES" : "NO");
                col.put("columnKey", rs.getString("Key"));
                col.put("columnComment", rs.getString("Comment") == null ? "" : rs.getString("Comment"));
                columns.add(col);
            }
        }
        return columns;
    }

    /**
     * 从列信息直接生成 DDL（不再次连接）
     */
    public String generateDdlFromColumns(List<Map<String, String>> columns, String targetDatabase, String targetTable) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS `").append(targetDatabase).append("`.`").append(targetTable).append("` (\n");
        List<String> primaryKeys = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Map<String, String> col = columns.get(i);
            ddl.append("  `").append(col.get("columnName")).append("` ").append(col.get("srType"));
            if ("PRI".equalsIgnoreCase(col.get("columnKey"))) ddl.append(" NOT NULL");
            String comment = col.get("columnComment");
            if (comment != null && !comment.isEmpty()) ddl.append(" COMMENT '").append(escapeComment(comment)).append("'");
            ddl.append(i < columns.size() - 1 ? ",\n" : "\n");
            if ("PRI".equalsIgnoreCase(col.get("columnKey"))) primaryKeys.add(col.get("columnName"));
        }
        ddl.append(") ENGINE=OLAP\n");
        if (!primaryKeys.isEmpty()) {
            ddl.append("PRIMARY KEY(").append(primaryKeys.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", "))).append(")\n");
        }
        ddl.append("DISTRIBUTED BY HASH(`");
        ddl.append(primaryKeys.isEmpty() ? columns.get(0).get("columnName") : primaryKeys.get(0));
        ddl.append("`) BUCKETS 3\n");
        ddl.append("PROPERTIES (\n  \"replication_num\" = \"3\"\n);");
        return ddl.toString();
    }

    /**
     * 刷新元数据（此处简单重新查询数据库列表以模拟刷新，实际可引入缓存后失效缓存）
     */
    public void refreshMetadata(Long datasourceId) {
        // 本示例无缓存，直接调用 getDatabases 即可，这里额外打印日志表示刷新操作
        System.out.println("刷新数据源 ID: " + datasourceId + " 的元数据");
        // 可以在此添加缓存清除逻辑
    }

    public Map<String, Object> previewData(Long datasourceId, String database, String tableName) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
        String sql = "SELECT * FROM " + quoteIdentifier(tableName) + " LIMIT 5";
        List<String> columns = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                columns.add(meta.getColumnLabel(i));
            }
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String col : columns) {
                    row.put(col, rs.getObject(col));
                }
                rows.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("获取预览数据失败: " + e.getMessage(), e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put("columns", columns);
        result.put("rows", rows);
        return result;
    }

    /**
     * 执行 SQL（仅限 StarRocks 数据源，用于增删改查）
     * @return 受影响行数 或 查询结果（仅 SELECT）
     */
    public Map<String, Object> executeSql(Long datasourceId, String database, String sql) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        if (!"starrocks".equalsIgnoreCase(ds.getType())) {
            throw new RuntimeException("仅 StarRocks 数据源支持执行 SQL");
        }
        // 禁用危险操作
        String upperSql = sql.trim().toUpperCase();
        if (upperSql.startsWith("DROP DATABASE") || upperSql.startsWith("ALTER SYSTEM")
                || upperSql.contains("SHUTDOWN")) {
            throw new RuntimeException("不允许执行高危操作");
        }

        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
        Map<String, Object> result = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword())) {
            boolean isSelect = upperSql.trim().startsWith("SELECT") || upperSql.trim().startsWith("SHOW")
                    || upperSql.trim().startsWith("DESC") || upperSql.trim().startsWith("EXPLAIN");

            if (isSelect) {
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        columns.add(meta.getColumnLabel(i));
                    }
                    List<Map<String, Object>> rows = new ArrayList<>();
                    int limit = 0;
                    while (rs.next() && limit++ < 1000) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (String col : columns) {
                            row.put(col, rs.getObject(col));
                        }
                        rows.add(row);
                    }
                    result.put("columns", columns);
                    result.put("rows", rows);
                    result.put("type", "query");
                }
            } else {
                try (Statement stmt = conn.createStatement()) {
                    int affected = stmt.executeUpdate(sql);
                    result.put("affectedRows", affected);
                    result.put("type", "update");
                }
            }
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("message", e.getMessage());
        }
        return result;
    }

    /**
     * 删除表（仅限 StarRocks）
     */
    public void dropTable(Long datasourceId, String database, String tableName) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        if (!"starrocks".equalsIgnoreCase(ds.getType())) {
            throw new RuntimeException("仅 StarRocks 数据源支持删除表");
        }
        String sql = "DROP TABLE IF EXISTS " + quoteIdentifier(database) + "." + quoteIdentifier(tableName);
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            throw new RuntimeException("删除表失败: " + e.getMessage(), e);
        }
    }

    /** 查询表全部数据 */
    public Map<String, Object> queryTableData(Long datasourceId, String database, String tableName, int limit) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        String url = buildUrl(ds, database);
        int safeLimit = Math.min(limit, 200); // 最多 200 条
        String sql = "SELECT * FROM " + quoteIdentifier(tableName) + " LIMIT " + safeLimit;
        return executeQuery(ds, url, sql);
    }

    /** 新增行（仅限 StarRocks） */
    public void insertRow(Long datasourceId, String database, String tableName, Map<String, Object> values) {
        Datasource ds = checkStarRocks(datasourceId);
        String url = buildUrl(ds, database);
        StringBuilder cols = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!first) { cols.append(", "); vals.append(", "); }
            cols.append(quoteIdentifier(entry.getKey()));
            vals.append(quoteValue(entry.getValue()));
            first = false;
        }
        String sql = "INSERT INTO " + quoteIdentifier(tableName) + " (" + cols + ") VALUES (" + vals + ")";
        executeUpdate(ds, url, sql);
    }

    /** 更新行（仅限 StarRocks） */
    public void updateRow(Long datasourceId, String database, String tableName,
                          String pkColumn, String pkValue, Map<String, Object> values) {
        Datasource ds = checkStarRocks(datasourceId);
        String url = buildUrl(ds, database);
        StringBuilder sets = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getKey().equals(pkColumn)) continue;
            if (!first) sets.append(", ");
            sets.append(quoteIdentifier(entry.getKey())).append(" = ").append(quoteValue(entry.getValue()));
            first = false;
        }
        String sql = "UPDATE " + quoteIdentifier(tableName) + " SET " + sets + " WHERE " + quoteIdentifier(pkColumn) + " = " + quoteValue(pkValue);
        executeUpdate(ds, url, sql);
    }

    /** 删除行（仅限 StarRocks） */
    public void deleteRow(Long datasourceId, String database, String tableName,
                          String pkColumn, String pkValue) {
        Datasource ds = checkStarRocks(datasourceId);
        String url = buildUrl(ds, database);
        String sql = "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + quoteIdentifier(pkColumn) + " = " + quoteValue(pkValue);
        executeUpdate(ds, url, sql);
    }

    // ===== 内部工具方法 =====

    private String buildUrl(Datasource ds, String database) {
        return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                ds.getHost(), ds.getPort(), database);
    }

    private Datasource checkStarRocks(Long datasourceId) {
        Datasource ds = datasourceService.getById(datasourceId);
        if (ds == null) throw new RuntimeException("数据源不存在");
        if (!"starrocks".equalsIgnoreCase(ds.getType())) {
            throw new RuntimeException("仅 StarRocks 数据源支持此操作");
        }
        return ds;
    }

    /** 转义标识符（表名、列名），防止 SQL 注入 */
    private String quoteIdentifier(String identifier) {
        if (identifier == null) return "``";
        // 先把已有的反引号转义，然后包裹反引号
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String quoteValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof Number || val instanceof Boolean) return val.toString();
        return "'" + val.toString().replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    /** 转义注释中的单引号 */
    private String escapeComment(String comment) {
        if (comment == null) return "";
        return comment.replace("\\", "\\\\").replace("'", "\\'");
    }

    private Map<String, Object> executeQuery(Datasource ds, String url, String sql) {
        Map<String, Object> result = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= colCount; i++) columns.add(meta.getColumnLabel(i));
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (String col : columns) row.put(col, rs.getObject(col));
                rows.add(row);
            }
            result.put("columns", columns);
            result.put("rows", rows);
        } catch (Exception e) {
            throw new RuntimeException("查询失败: " + e.getMessage(), e);
        }
        return result;
    }

    private void executeUpdate(Datasource ds, String url, String sql) {
        try (Connection conn = DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            throw new RuntimeException("操作失败: " + e.getMessage(), e);
        }
    }
}