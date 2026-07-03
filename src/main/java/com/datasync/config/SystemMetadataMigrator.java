package com.datasync.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Idempotent system-table migrations for existing online deployments.
 *
 * Hibernate ddl-auto=update covers many entity changes, but production databases
 * can drift. Keep these migrations additive only: no DROP, no table rebuilds,
 * and no destructive type changes.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class SystemMetadataMigrator implements CommandLineRunner {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void run(String... args) {
        migrateSyncTask();
        migrateAlertConfig();
        migrateIndexes();
    }

    private void migrateSyncTask() {
        addColumnIfMissing("sync_task", "schema_save_mode",
                "ALTER TABLE sync_task ADD COLUMN schema_save_mode VARCHAR(50) DEFAULT 'CREATE_SCHEMA_WHEN_NOT_EXIST' COMMENT '目标表结构处理策略'");
        addColumnIfMissing("sync_task", "data_save_mode",
                "ALTER TABLE sync_task ADD COLUMN data_save_mode VARCHAR(50) DEFAULT 'APPEND_DATA' COMMENT '目标表数据处理策略'");

        jdbcTemplate.update("UPDATE sync_task SET schema_save_mode = 'CREATE_SCHEMA_WHEN_NOT_EXIST' WHERE schema_save_mode IS NULL OR schema_save_mode = ''");
        jdbcTemplate.update("UPDATE sync_task SET data_save_mode = 'APPEND_DATA' WHERE data_save_mode IS NULL OR data_save_mode = ''");
    }

    private void migrateAlertConfig() {
        if (!tableExists("alert_config")) {
            return;
        }
        addColumnIfMissing("alert_config", "event_types",
                "ALTER TABLE alert_config ADD COLUMN event_types VARCHAR(50) NOT NULL DEFAULT 'FAILURE'");
        addColumnIfMissing("alert_config", "enabled",
                "ALTER TABLE alert_config ADD COLUMN enabled TINYINT(1) NOT NULL DEFAULT 1");
        addColumnIfMissing("alert_config", "updated_at",
                "ALTER TABLE alert_config ADD COLUMN updated_at TIMESTAMP NULL");

        jdbcTemplate.update("UPDATE alert_config SET event_types = 'FAILURE' WHERE event_types IS NULL OR event_types = ''");
        jdbcTemplate.update("UPDATE alert_config SET enabled = 1 WHERE enabled IS NULL");
    }

    private void migrateIndexes() {
        addIndexIfMissing("sync_task", "idx_sync_task_type_status",
                "CREATE INDEX idx_sync_task_type_status ON sync_task(sync_type, last_exec_status)");
        addIndexIfMissing("task_execution", "idx_task_execution_started_status",
                "CREATE INDEX idx_task_execution_started_status ON task_execution(started_at, status)");
        addIndexIfMissing("task_execution", "idx_task_execution_task_started",
                "CREATE INDEX idx_task_execution_task_started ON task_execution(task_id, started_at)");
    }

    private void addColumnIfMissing(String tableName, String columnName, String ddl) {
        if (!tableExists(tableName) || columnExists(tableName, columnName)) {
            return;
        }
        jdbcTemplate.execute(ddl);
        System.out.println("[Migration] added column " + tableName + "." + columnName);
    }

    private void addIndexIfMissing(String tableName, String indexName, String ddl) {
        if (!tableExists(tableName) || indexExists(tableName, indexName)) {
            return;
        }
        jdbcTemplate.execute(ddl);
        System.out.println("[Migration] added index " + tableName + "." + indexName);
    }

    private boolean tableExists(String tableName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
                Integer.class, tableName);
        return count != null && count > 0;
    }

    private boolean columnExists(String tableName, String columnName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
                Integer.class, tableName, columnName);
        return count != null && count > 0;
    }

    private boolean indexExists(String tableName, String indexName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
                Integer.class, tableName, indexName);
        return count != null && count > 0;
    }
}
