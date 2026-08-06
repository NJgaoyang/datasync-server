package com.datasync.service;

import com.datasync.entity.*;
import com.datasync.repository.*;
import com.datasync.util.AesUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class SyncTaskService {

    @Value("${datasync.secret-key:DataSync@2026!Key}")
    private String secretKey;

    @Autowired
    private SyncTaskRepository taskRepository;
    @Autowired
    private SyncTaskTableRepository tableRepository;
    @Autowired
    private DatasourceService datasourceService;
    @Autowired
    private MetadataService metadataService;
    @Autowired
    private SchedulerService schedulerService;
    @Autowired
    private TaskExecutionRepository executionRepository;

    @Transactional
    public SyncTask createTask(Map<String, Object> params) {
        String taskName = (String) params.get("taskName");
        Long sourceId = Long.valueOf(params.get("sourceId").toString());
        Long targetId = Long.valueOf(params.get("targetId").toString());
        String syncType = (String) params.get("syncType");
        String engine = (String) params.get("engine");
        String deployMode = (String) params.get("deployMode");
        String description = (String) params.get("description");

        Datasource sourceDs = datasourceService.getById(sourceId);
        Datasource targetDs = datasourceService.getById(targetId);
        if (sourceDs == null || targetDs == null) throw new RuntimeException("数据源不存在");

        SyncTask task = new SyncTask();
        task.setTaskName(taskName);
        task.setSyncType(syncType);
        task.setEngine(engine);
        task.setDeployMode(deployMode);
        task.setSourceId(sourceId);
        task.setTargetId(targetId);
        if (params.containsKey("clusterId") && params.get("clusterId") != null) {
            task.setClusterId(Long.valueOf(params.get("clusterId").toString()));
        }
        task.setSourceName(sourceDs.getName());
        task.setTargetName(targetDs.getName());
        task.setStatus("DRAFT");
        task.setDescription(description);
        task.setEnabled(false);
        task.setLastExecStatus("NONE");
        // 增量WHERE（自由输入，为空则全量同步）
        task.setIncrementalWhere((String) params.getOrDefault("incrementalWhere", null));
        applySaveModes(task, params);
        taskRepository.save(task);

        // 保存表映射（一次连接完成 DDL + 列列表缓存）
        List<Map<String, Object>> tables = (List<Map<String, Object>>) params.get("tables");
        Map<String, String> colListCache = new HashMap<>();
        if (tables != null && !tables.isEmpty()) {
            String srcDb = (String) tables.get(0).get("sourceDatabase");
            Datasource srcDs = datasourceService.getById(sourceId);
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                    srcDs.getHost(), srcDs.getPort(), srcDb);
            try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, srcDs.getUsername(), srcDs.getPassword())) {
                for (Map<String, Object> t : tables) {
                    SyncTaskTable tt = new SyncTaskTable();
                    tt.setTaskId(task.getId());
                    tt.setSourceDatabase((String) t.get("sourceDatabase"));
                    tt.setSourceTable((String) t.get("sourceTable"));
                    tt.setTargetDatabase((String) t.get("targetDatabase"));
                    tt.setTargetTable((String) t.get("targetTable"));
                    tt.setPartitionColumn((String) t.getOrDefault("partitionColumn", ""));
                    tt.setSyncType(syncType);
                    try {
                        List<Map<String, String>> cols = metadataService.getColumnsFromConnection(conn, tt.getSourceTable());
                        tt.setSrDdl(metadataService.generateDdlFromColumns(cols, tt.getTargetDatabase(), tt.getTargetTable()));
                        colListCache.put(tt.getSourceDatabase() + "." + tt.getSourceTable(),
                                cols.stream().map(c -> "`" + c.get("columnName") + "`").collect(Collectors.joining(", ")));
                    } catch (Exception e) {
                        tt.setSrDdl("-- DDL生成失败: " + e.getMessage());
                    }
                    tableRepository.save(tt);
                }
            } catch (Exception ex) {
                throw new RuntimeException("连接源数据库失败: " + ex.getMessage(), ex);
            }
        }

        // 实时任务高级参数
        Map<String, Object> rtConfig = new HashMap<>();
        if ("realtime".equals(syncType)) {
            for (String k : new String[]{"rtParallelism","rtStartupMode","rtStartupTimestamp",
                    "rtCheckpointSec","rtBatchMaxRows","rtBatchMaxBytes",
                    "rtSchemaSaveMode","rtDataSaveMode"}) {
                if (params.containsKey(k)) rtConfig.put(k, params.get(k));
            }
        }

        String config = buildSeaTunnelConfig(task, tables, colListCache, rtConfig);
        task.setSeatunnelConfig(config);
        task.setStatus("GENERATED");
        taskRepository.save(task);
        return task;
    }

    public SyncTask updateTask(Map<String, Object> params) {
        Long id = Long.valueOf(params.get("id").toString());
        SyncTask task = taskRepository.findById(id).orElseThrow(() -> new RuntimeException("任务不存在"));
        if (params.containsKey("taskName")) task.setTaskName((String) params.get("taskName"));
        if (params.containsKey("engine")) task.setEngine((String) params.get("engine"));
        if (params.containsKey("deployMode")) task.setDeployMode((String) params.get("deployMode"));
        if (params.containsKey("description")) task.setDescription((String) params.get("description"));
        if (params.containsKey("clusterId")) task.setClusterId(params.get("clusterId") != null ? Long.valueOf(params.get("clusterId").toString()) : null);
        if (params.containsKey("seatunnelConfig")) task.setSeatunnelConfig((String) params.get("seatunnelConfig"));
        if (params.containsKey("incrementalWhere")) task.setIncrementalWhere((String) params.get("incrementalWhere"));
        applySaveModes(task, params);

        // 重新生成所有表的 DDL + 列列表 + SeaTunnel 配置（一次连接）
        List<SyncTaskTable> tables = tableRepository.findByTaskId(id);
        if (!tables.isEmpty()) {
            Datasource srcDs = datasourceService.getById(task.getSourceId());
            String srcDb = tables.get(0).getSourceDatabase();
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                    srcDs.getHost(), srcDs.getPort(), srcDb);
            try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, srcDs.getUsername(), srcDs.getPassword())) {
                Map<String, String> colListCache = new HashMap<>();
                for (SyncTaskTable tt : tables) {
                    try {
                        List<Map<String, String>> cols = metadataService.getColumnsFromConnection(conn, tt.getSourceTable());
                        tt.setSrDdl(metadataService.generateDdlFromColumns(cols, tt.getTargetDatabase(), tt.getTargetTable()));
                        // 缓存列列表
                        String key = tt.getSourceDatabase() + "." + tt.getSourceTable();
                        colListCache.put(key, cols.stream().map(c -> "`" + c.get("columnName") + "`").collect(Collectors.joining(", ")));
                    } catch (Exception ignored) {}
                    tableRepository.save(tt);
                }
                // 用缓存的列列表生成配置
                List<Map<String, Object>> tableMaps = tables.stream().map(tt -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("sourceDatabase", tt.getSourceDatabase());
                    m.put("sourceTable", tt.getSourceTable());
                    m.put("targetDatabase", tt.getTargetDatabase());
                    m.put("targetTable", tt.getTargetTable());
                    return m;
                }).collect(Collectors.toList());
                if (!tableMaps.isEmpty()) {
                    task.setSeatunnelConfig(buildSeaTunnelConfig(task, tableMaps, colListCache));
                }
            } catch (Exception ignored) {}
        }
        task.setStatus("GENERATED");

        return taskRepository.save(task);
    }

    private void applySaveModes(SyncTask task, Map<String, Object> params) {
        String schemaMode = getModeParam(params, "schemaSaveMode",
                getModeParam(params, "rtSchemaSaveMode", task.getSchemaSaveMode()));
        String dataMode = getModeParam(params, "dataSaveMode",
                getModeParam(params, "rtDataSaveMode", task.getDataSaveMode()));
        task.setSchemaSaveMode(normalizeSchemaSaveMode(schemaMode));
        task.setDataSaveMode(normalizeDataSaveMode(dataMode));
    }

    private String getModeParam(Map<String, Object> params, String key, String defaultValue) {
        Object value = params.get(key);
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            return defaultValue;
        }
        return String.valueOf(value).trim();
    }

    private String normalizeSchemaSaveMode(String value) {
        if ("RECREATE_SCHEMA".equals(value) || "ERROR_WHEN_SCHEMA_NOT_EXIST".equals(value) || "IGNORE".equals(value)) {
            return value;
        }
        return "CREATE_SCHEMA_WHEN_NOT_EXIST";
    }

    private String normalizeDataSaveMode(String value) {
        if ("DROP_DATA".equals(value) || "ERROR_WHEN_DATA_EXISTS".equals(value)) {
            return value;
        }
        return "APPEND_DATA";
    }

    public List<SyncTask> listTasks() {
        return taskRepository.findAll();
    }

    public List<Map<String, Object>> listTaskSummaries() {
        return taskRepository.findTaskSummaries().stream()
                .map(this::mapTaskSummary)
                .collect(Collectors.toList());
    }

    public Map<String, Object> getDashboardStats() {
        Map<String, Map<String, Long>> stats = new LinkedHashMap<>();
        stats.put("all", emptyDashboardBucket());
        stats.put("batch", emptyDashboardBucket());
        stats.put("realtime", emptyDashboardBucket());

        for (Object[] row : taskRepository.getDashboardStats()) {
            String syncType = row[0] == null ? "unknown" : row[0].toString().toLowerCase();
            String status = row[1] == null ? "NONE" : row[1].toString().toUpperCase();
            long count = ((Number) row[2]).longValue();

            addDashboardCount(stats.get("all"), status, count);
            Map<String, Long> typeBucket = stats.get(syncType);
            if (typeBucket != null) {
                addDashboardCount(typeBucket, status, count);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("all", stats.get("all"));
        result.put("batch", stats.get("batch"));
        result.put("realtime", stats.get("realtime"));
        return result;
    }

    private Map<String, Long> emptyDashboardBucket() {
        Map<String, Long> bucket = new LinkedHashMap<>();
        bucket.put("total", 0L);
        bucket.put("success", 0L);
        bucket.put("failed", 0L);
        bucket.put("running", 0L);
        bucket.put("pending", 0L);
        bucket.put("none", 0L);
        return bucket;
    }

    private void addDashboardCount(Map<String, Long> bucket, String status, long count) {
        bucket.put("total", bucket.get("total") + count);
        if ("SUCCESS".equals(status)) {
            bucket.put("success", bucket.get("success") + count);
        } else if ("FAILED".equals(status)) {
            bucket.put("failed", bucket.get("failed") + count);
        } else if ("RUNNING".equals(status)) {
            bucket.put("running", bucket.get("running") + count);
        } else if ("PENDING".equals(status)) {
            bucket.put("pending", bucket.get("pending") + count);
        } else {
            bucket.put("none", bucket.get("none") + count);
        }
    }

    private Map<String, Object> mapTaskSummary(Object[] row) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", row[0]);
        item.put("taskName", row[1]);
        item.put("syncType", row[2]);
        item.put("engine", row[3]);
        item.put("deployMode", row[4]);
        item.put("sourceId", row[5]);
        item.put("targetId", row[6]);
        item.put("sourceName", row[7]);
        item.put("targetName", row[8]);
        item.put("status", row[9]);
        item.put("clusterId", row[10]);
        item.put("description", row[11]);
        item.put("enabled", toBoolean(row[12]));
        item.put("cronExpression", row[13]);
        item.put("scheduleEnabled", toBoolean(row[14]));
        item.put("lastExecStatus", row[15]);
        item.put("lastExecTime", row[16] != null ? row[16].toString() : null);
        item.put("lastExecDuration", row[17]);
        item.put("lastExecRows", row[18]);
        item.put("lastExecQps", row[19]);
        item.put("incrementalWhere", row[20]);
        item.put("schemaSaveMode", row[21]);
        item.put("dataSaveMode", row[22]);
        item.put("createdBy", row[23]);
        item.put("createdAt", row[24] != null ? row[24].toString() : null);
        item.put("updatedAt", row[25] != null ? row[25].toString() : null);
        return item;
    }

    private Boolean toBoolean(Object value) {
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).intValue() != 0;
        return Boolean.parseBoolean(value.toString());
    }

    public SyncTask setCron(Map<String, Object> params) {
        Long id = Long.valueOf(params.get("id").toString());
        SyncTask task = taskRepository.findById(id).orElseThrow(() -> new RuntimeException("任务不存在"));
        String cron = (String) params.get("cronExpression");
        boolean hadCron = task.getCronExpression() != null && !task.getCronExpression().trim().isEmpty();
        String newCron = (cron != null && cron.trim().isEmpty()) ? null : cron;
        task.setCronExpression(newCron);
        task = taskRepository.save(task);
        // 如果 cron 被清除，取消调度；如果有新 cron 且调度和任务都开启，重新调度
        if (newCron == null) {
            schedulerService.cancelCronTask(id);
        } else if (Boolean.TRUE.equals(task.getEnabled()) && Boolean.TRUE.equals(task.getScheduleEnabled())) {
            schedulerService.scheduleCronTask(task);
        }
        return task;
    }

    public List<SyncTask> searchTasks(String keyword) {
        if (keyword == null || keyword.trim().isEmpty()) {
            return taskRepository.findAll();
        }
        return taskRepository.search(keyword.trim());
    }

    public SyncTask getTask(Long id) {
        return taskRepository.findById(id).orElse(null);
    }

    public List<SyncTaskTable> getTaskTables(Long taskId) {
        return tableRepository.findByTaskId(taskId);
    }

    public List<Map<String, Object>> listSyncedTables() {
        return tableRepository.findSyncedTableSummaries().stream()
                .map(this::mapSyncedTableSummary)
                .collect(Collectors.toList());
    }

    private Map<String, Object> mapSyncedTableSummary(Object[] row) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", row[0]);
        item.put("taskId", row[1]);
        item.put("taskName", row[2]);
        item.put("syncType", row[3]);
        item.put("enabled", toBoolean(row[4]));
        item.put("lastExecStatus", row[5]);
        item.put("sourceDatabase", row[6]);
        item.put("sourceTable", row[7]);
        item.put("targetDatabase", row[8]);
        item.put("targetTable", row[9]);
        item.put("createdAt", row[10] != null ? row[10].toString() : null);
        return item;
    }

    @Transactional
    public void deleteTask(Long id) {
        schedulerService.cancelCronTask(id);
        schedulerService.stopTask(id);
        tableRepository.deleteByTaskId(id);
        taskRepository.deleteById(id);
    }

    @Transactional
    public SyncTask updateTask(Long taskId, Map<String, Object> params) {
        SyncTask task = getTask(taskId);
        if (task == null) throw new RuntimeException("任务不存在");
        if ("RUNNING".equals(task.getStatus())) {
            throw new RuntimeException("任务运行中，不能修改");
        }
        if (params.containsKey("taskName")) task.setTaskName((String) params.get("taskName"));
        if (params.containsKey("description")) task.setDescription((String) params.get("description"));
        if (params.containsKey("incrementalWhere")) task.setIncrementalWhere((String) params.get("incrementalWhere"));
        if (params.containsKey("engine")) task.setEngine((String) params.get("engine"));
        if (params.containsKey("deployMode")) task.setDeployMode((String) params.get("deployMode"));
        if (params.containsKey("clusterId")) task.setClusterId(params.get("clusterId") != null ? Long.valueOf(params.get("clusterId").toString()) : null);
        applySaveModes(task, params);

        List<SyncTaskTable> oldTables = tableRepository.findByTaskId(taskId);
        String sourceDatabase = oldTables.isEmpty() ? "" : oldTables.get(0).getSourceDatabase();
        tableRepository.deleteByTaskId(taskId);

        String targetDatabase = params.containsKey("targetDatabase") ? (String) params.get("targetDatabase") : "";
        if (targetDatabase.isEmpty() && !oldTables.isEmpty()) {
            targetDatabase = oldTables.get(0).getTargetDatabase();
        }

        List<Map<String, Object>> tables = (List<Map<String, Object>>) params.get("tables");
        if (tables != null && !tables.isEmpty()) {
            Datasource srcDs = datasourceService.getById(task.getSourceId());
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                    srcDs.getHost(), srcDs.getPort(), sourceDatabase);
            try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, srcDs.getUsername(), srcDs.getPassword())) {
                for (Map<String, Object> t : tables) {
                    SyncTaskTable tt = new SyncTaskTable();
                    tt.setTaskId(taskId);
                    tt.setSourceDatabase(sourceDatabase);
                    tt.setSourceTable((String) t.get("sourceTable"));
                    tt.setTargetDatabase(targetDatabase);
                    tt.setTargetTable((String) t.get("targetTable"));
                    tt.setPartitionColumn((String) t.getOrDefault("partitionColumn", ""));
                    tt.setSyncType(task.getSyncType());
                    try {
                        List<Map<String, String>> cols = metadataService.getColumnsFromConnection(conn, tt.getSourceTable());
                        String ddl = metadataService.generateDdlFromColumns(cols, tt.getTargetDatabase(), tt.getTargetTable());
                        tt.setSrDdl(ddl);
                    } catch (Exception e) {
                        tt.setSrDdl("-- DDL生成失败: " + e.getMessage());
                    }
                    tableRepository.save(tt);
                }
            } catch (Exception ex) {
                throw new RuntimeException("连接源数据库失败: " + ex.getMessage(), ex);
            }
        }

        String config = buildSeaTunnelConfig(task, tables);
        task.setSeatunnelConfig(config);
        task.setStatus("GENERATED");
        taskRepository.save(task);
        return task;
    }

    @Transactional
    public SyncTask toggleEnabled(Long taskId, boolean enabled) {
        SyncTask task = getTask(taskId);
        if (task == null) throw new RuntimeException("任务不存在");
        task.setEnabled(enabled);
        if (enabled) {
            // 上线时恢复定时调度（不重置 scheduleEnabled，保持用户原设置）
            if (Boolean.TRUE.equals(task.getScheduleEnabled()) && task.getCronExpression() != null
                    && !task.getCronExpression().trim().isEmpty()) {
                schedulerService.scheduleCronTask(task);
            }
        } else {
            // 下线时只取消调度执行，不修改 scheduleEnabled 状态
            schedulerService.cancelCronTask(taskId);
            schedulerService.stopTask(taskId);
        }
        return taskRepository.save(task);
    }

    @Transactional
    public SyncTask toggleSchedule(Long taskId, boolean scheduleEnabled) {
        SyncTask task = getTask(taskId);
        if (task == null) throw new RuntimeException("任务不存在");
        task.setScheduleEnabled(scheduleEnabled);
        task = taskRepository.save(task);
        if (scheduleEnabled && Boolean.TRUE.equals(task.getEnabled()) && task.getCronExpression() != null) {
            schedulerService.scheduleCronTask(task);
        } else {
            schedulerService.cancelCronTask(taskId);
        }
        return task;
    }

    @Transactional
    public void executeTask(Long taskId) {
        schedulerService.executeTask(taskId);
    }

    /**
     * 重新生成任务的 SeaTunnel 配置（用最新代码逻辑，解决旧配置参数过期问题）
     */
    public void regenerateConfig(Long taskId) {
        SyncTask task = taskRepository.findById(taskId).orElseThrow(() -> new RuntimeException("任务不存在"));
        List<SyncTaskTable> tables = tableRepository.findByTaskIdOrderByIdAsc(taskId);
        if (tables.isEmpty()) return;

        // 构建 tables 列表
        List<Map<String, Object>> tableMaps = toSeaTunnelTableMaps(tables);

        // 实时运行参数使用默认值，表结构/数据保存策略使用任务持久化配置
        Map<String, Object> rtConfig = new HashMap<>();
        if ("realtime".equals(task.getSyncType())) {
            rtConfig.put("rtParallelism", 2);
            rtConfig.put("rtStartupMode", "initial");
            rtConfig.put("rtCheckpointSec", 5);
            rtConfig.put("rtSchemaSaveMode", task.getSchemaSaveMode());
            rtConfig.put("rtDataSaveMode", task.getDataSaveMode());
        }

        String config = buildSeaTunnelConfig(task, tableMaps, null, rtConfig);
        task.setSeatunnelConfig(config);
        taskRepository.save(task);
    }

    /**
     * 删除任务中的一张表，并重新生成 SeaTunnel 配置（剔除已删除的表）
     */
    @Transactional
    public SyncTask deleteTaskTable(Long taskId, Long tableId) {
        SyncTask task = taskRepository.findById(taskId).orElseThrow(() -> new RuntimeException("任务不存在"));
        if (Boolean.TRUE.equals(task.getEnabled())) {
            throw new RuntimeException("任务在线，请先下线后再删除表");
        }
        if ("RUNNING".equals(task.getStatus())) {
            throw new RuntimeException("任务运行中，无法删除表");
        }
        SyncTaskTable table = tableRepository.findById(tableId).orElseThrow(() -> new RuntimeException("表不存在"));
        if (!Objects.equals(table.getTaskId(), taskId)) {
            throw new RuntimeException("表不属于该任务");
        }
        tableRepository.delete(table);

        // 删除后重新生成配置，跳过目标库结构准备（避免误触发建表/删表）
        List<SyncTaskTable> remaining = tableRepository.findByTaskIdOrderByIdAsc(taskId);
        if (remaining.isEmpty()) {
            task.setSeatunnelConfig("");
        } else {
            task.setSeatunnelConfig(buildSeaTunnelConfigForTables(task, remaining, true));
        }
        return taskRepository.save(task);
    }

    public String buildSeaTunnelConfigForTables(SyncTask task, List<SyncTaskTable> tables) {
        return buildSeaTunnelConfigForTables(task, tables, false);
    }

    /**
     * 生成 SeaTunnel 配置；skipSchemaPrepare=true 时跳过目标库表结构准备（用于删除表等纯配置管理操作，避免误触发建表/删表）
     */
    private String buildSeaTunnelConfigForTables(SyncTask task, List<SyncTaskTable> tables, boolean skipSchemaPrepare) {
        if (task == null) throw new RuntimeException("浠诲姟涓嶅瓨鍦?");
        if (tables == null || tables.isEmpty()) throw new RuntimeException("浠诲姟娌℃湁鍏宠仈琛?");

        Map<String, Object> rtConfig = new HashMap<>();
        if ("realtime".equals(task.getSyncType())) {
            rtConfig.put("rtParallelism", 2);
            rtConfig.put("rtStartupMode", "initial");
            rtConfig.put("rtCheckpointSec", 5);
            rtConfig.put("rtSchemaSaveMode", task.getSchemaSaveMode());
            rtConfig.put("rtDataSaveMode", task.getDataSaveMode());
        }

        return buildSeaTunnelConfig(task, toSeaTunnelTableMaps(tables), null, rtConfig, skipSchemaPrepare);
    }

    private List<Map<String, Object>> toSeaTunnelTableMaps(List<SyncTaskTable> tables) {
        return tables.stream()
                .sorted(Comparator.comparing(SyncTaskTable::getId, Comparator.nullsLast(Long::compareTo)))
                .map(t -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("sourceDatabase", t.getSourceDatabase());
                    m.put("sourceTable", t.getSourceTable());
                    m.put("targetDatabase", t.getTargetDatabase());
                    m.put("targetTable", t.getTargetTable());
                    return m;
                }).collect(Collectors.toList());
    }

    @Transactional
    public void stopTask(Long taskId) {
        schedulerService.stopTask(taskId);
    }

    public Map<String, Object> getTaskStatus(Long taskId) {
        SyncTask task = getTask(taskId);
        if (task == null) throw new RuntimeException("任务不存在");
        Map<String, Object> result = new HashMap<>();
        result.put("enabled", task.getEnabled());
        result.put("lastExecStatus", task.getLastExecStatus());
        result.put("lastExecTime", task.getLastExecTime());
        result.put("running", schedulerService.isTaskRunning(taskId));
        return result;
    }

    public List<TaskExecution> getExecutionHistory(Long taskId, int limit) {
        return schedulerService.getExecutionHistory(taskId, limit);
    }

    public TaskExecution getExecution(Long execId) {
        return schedulerService.getExecution(execId);
    }

    /** 任务日志分页（关联任务名称和类型，支持日期+状态筛选） */
    public Map<String, Object> getExecutionsPage(String keyword, int page, int size, String startDate, String endDate, String status) {
        Pageable pageable = PageRequest.of(page, size);
        String kw = (keyword == null || keyword.trim().isEmpty()) ? null : keyword.trim();
        String sd = (startDate == null || startDate.trim().isEmpty()) ? null : startDate.trim();
        String ed = (endDate == null || endDate.trim().isEmpty()) ? null : endDate.trim();
        String st = (status == null || status.trim().isEmpty()) ? null : status.trim();
        Page<Object[]> result = executionRepository.findAllWithTaskName(kw, sd, ed, st, pageable);

        List<Map<String, Object>> list = new ArrayList<>();
        for (Object[] row : result.getContent()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", row[0]);           // e.id
            item.put("taskId", row[1]);       // e.task_id
            item.put("status", row[2]);       // e.status
            item.put("logText", row[3]);      // e.log_text
            item.put("startedAt", row[4] != null ? row[4].toString() : null);    // e.started_at
            item.put("finishedAt", row[5] != null ? row[5].toString() : null);   // e.finished_at
            item.put("triggeredBy", row[6]);  // e.triggered_by
            item.put("taskName", row[7]);     // t.task_name
            item.put("syncType", row[8]);     // t.sync_type
            item.put("createdBy", row[9]);    // t.created_by
            list.add(item);
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("content", list);
        resultMap.put("totalElements", result.getTotalElements());
        resultMap.put("totalPages", result.getTotalPages());
        resultMap.put("number", result.getNumber());
        return resultMap;
    }

    /** 批量删除任务执行记录 */
    @Transactional
    public int deleteExecutions(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return 0;
        return executionRepository.deleteByIdIn(ids);
    }

    /**
     * 同步建表：在目标 StarRocks 上执行 DDL
     * @param taskId 任务ID
     * @param dropIfExists 是否先 DROP 再 CREATE（默认 false）
     * @return 每张表的执行结果
     */
    public List<Map<String, Object>> syncDdl(Long taskId, boolean dropIfExists) {
        SyncTask task = getTask(taskId);
        if (task == null) throw new RuntimeException("任务不存在");

        Datasource targetDs = datasourceService.getById(task.getTargetId());
        if (targetDs == null) throw new RuntimeException("目标数据源不存在");

        List<SyncTaskTable> tables = tableRepository.findByTaskId(taskId);
        if (tables.isEmpty()) throw new RuntimeException("任务没有关联表");

        // 处理逗号分隔的多节点
        String[] hosts = targetDs.getHost().split(",");
        String jdbcHost = hosts[0].trim();
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true",
                jdbcHost, targetDs.getPort());

        List<Map<String, Object>> results = new ArrayList<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, targetDs.getUsername(), targetDs.getPassword())) {
            for (SyncTaskTable tt : tables) {
                Map<String, Object> result = new HashMap<>();
                result.put("table", tt.getTargetTable());
                String ddl = tt.getSrDdl();
                if (ddl == null || ddl.isEmpty()) {
                    result.put("success", false);
                    result.put("message", "DDL为空");
                    results.add(result);
                    continue;
                }
                try (java.sql.Statement stmt = conn.createStatement()) {
                    if (dropIfExists) {
                        stmt.execute("DROP TABLE IF EXISTS `" + tt.getTargetDatabase() + "`.`" + tt.getTargetTable() + "`");
                    }
                    // 替换 CREATE TABLE IF NOT EXISTS → CREATE TABLE（targetDb/table 已由 DDL 指定）
                    boolean exists = !dropIfExists && targetTableExists(conn, tt.getTargetDatabase(), tt.getTargetTable());
                    if (exists) {
                        List<String> addedColumns = addMissingTargetColumns(stmt, task, tt, conn);
                        result.put("success", true);
                        result.put("message", addedColumns.isEmpty()
                                ? "表已存在，结构已匹配"
                                : "表已存在，已补充字段: " + String.join(", ", addedColumns));
                    } else {
                        stmt.execute(ddl);
                        result.put("success", true);
                        result.put("message", dropIfExists ? "已删除并重建" : "建表成功");
                    }
                } catch (Exception e) {
                    result.put("success", false);
                    result.put("message", e.getMessage());
                }
                results.add(result);
            }
        } catch (Exception e) {
            throw new RuntimeException("连接目标数据源失败: " + e.getMessage(), e);
        }
        return results;
    }

    private boolean targetTableExists(java.sql.Connection conn, String database, String table) throws Exception {
        try (java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet ignored = stmt.executeQuery("SHOW FULL COLUMNS FROM "
                     + quoteIdentifier(database) + "." + quoteIdentifier(table))) {
            return true;
        } catch (Exception e) {
            if (isTableNotFound(e)) {
                return false;
            }
            throw e;
        }
    }

    private List<String> addMissingTargetColumns(java.sql.Statement stmt, SyncTask task, SyncTaskTable tt,
                                                 java.sql.Connection conn) throws Exception {
        return addMissingTargetColumns(conn, stmt, task,
                tt.getSourceDatabase(), tt.getSourceTable(), tt.getTargetDatabase(), tt.getTargetTable());
    }

    private List<String> addMissingTargetColumns(java.sql.Connection conn, java.sql.Statement stmt, SyncTask task,
                                                 String sourceDatabase, String sourceTable,
                                                 String targetDatabase, String targetTable) throws Exception {
        List<Map<String, String>> sourceColumns = metadataService.getColumns(
                task.getSourceId(), sourceDatabase, sourceTable);
        List<String> targetColumns = getTargetColumnNames(conn, targetDatabase, targetTable);
        List<String> addedColumns = new ArrayList<>();

        for (Map<String, String> sourceColumn : sourceColumns) {
            String columnName = sourceColumn.get("columnName");
            if (targetColumns.contains(columnName)) {
                continue;
            }
            if ("PRI".equalsIgnoreCase(sourceColumn.get("columnKey"))) {
                throw new RuntimeException("目标表已存在但缺少主键字段，无法安全自动补列: " + columnName);
            }
            String sql = "ALTER TABLE " + quoteIdentifier(targetDatabase) + "."
                    + quoteIdentifier(targetTable) + " ADD COLUMN "
                    + buildStarRocksColumnDefinition(sourceColumn);
            stmt.execute(sql);
            targetColumns.add(columnName);
            addedColumns.add(columnName);
        }
        return addedColumns;
    }

    private List<String> getTargetColumnNames(java.sql.Connection conn, String database, String table) throws Exception {
        List<String> columns = new ArrayList<>();
        try (java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SHOW FULL COLUMNS FROM "
                     + quoteIdentifier(database) + "." + quoteIdentifier(table))) {
            while (rs.next()) {
                columns.add(rs.getString("Field"));
            }
        }
        return columns;
    }

    private String buildStarRocksColumnDefinition(Map<String, String> column) {
        StringBuilder ddl = new StringBuilder();
        ddl.append(quoteIdentifier(column.get("columnName"))).append(" ").append(column.get("srType"));
        if ("PRI".equalsIgnoreCase(column.get("columnKey"))) {
            ddl.append(" NOT NULL");
        }
        String comment = column.get("columnComment");
        if (comment != null && !comment.isEmpty()) {
            ddl.append(" COMMENT '").append(escapeComment(comment)).append("'");
        }
        return ddl.toString();
    }

    private boolean isTableNotFound(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        return message.contains("doesn't exist") || message.contains("unknown table")
                || message.contains("unknown database") || message.contains("table not found");
    }

    private boolean isDatabaseNotFound(Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
        return message.contains("unknown database") || message.contains("database not found");
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables) {
        return buildSeaTunnelConfig(task, tables, null, new HashMap<>());
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables, Map<String, String> colListCache) {
        return buildSeaTunnelConfig(task, tables, colListCache, new HashMap<>());
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables, Map<String, String> colListCache, Map<String, Object> rtConfig) {
        return buildSeaTunnelConfig(task, tables, colListCache, rtConfig, false);
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables, Map<String, String> colListCache, Map<String, Object> rtConfig, boolean skipSchemaPrepare) {
        Datasource sourceDs = datasourceService.getById(task.getSourceId());
        Datasource targetDs = datasourceService.getById(task.getTargetId());
        String sourceUrl = String.format("jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&tinyInt1isBit=false",
                sourceDs.getHost(), sourceDs.getPort(), tables.get(0).get("sourceDatabase"));
        // 处理逗号分隔的多节点（如 172.16.0.100,172.16.0.101,172.16.0.102）
        String[] srHosts = targetDs.getHost().split(",");
        String srFirstHost = srHosts[0].trim();
        // base-url 用第一个节点（JDBC连接只需一个FE）
        String srBaseUrl = String.format("jdbc:mysql://%s:%d", srFirstHost, targetDs.getPort());
        // nodeUrls 多节点时拆分为数组
        StringBuilder srNodeUrlBuilder = new StringBuilder();
        for (int j = 0; j < srHosts.length; j++) {
            if (j > 0) srNodeUrlBuilder.append(", ");
            srNodeUrlBuilder.append("\"").append(srHosts[j].trim()).append(":8030\"");
        }
        String srNodeUrls = srNodeUrlBuilder.toString();

        // 加密密码
        String sourcePwd = AesUtil.encrypt(sourceDs.getPassword(), secretKey);
        String targetPwd = AesUtil.encrypt(targetDs.getPassword(), secretKey);

        // 多表时需要 plugin_output/plugin_input 建立映射
        boolean multiTable = tables.size() > 1;

        // 增量WHERE条件
        String incrWhere = task.getIncrementalWhere();
        boolean hasIncrWhere = incrWhere != null && !incrWhere.trim().isEmpty();

        String schemaSaveMode = normalizeSchemaSaveMode(task.getSchemaSaveMode());
        String dataSaveMode = normalizeDataSaveMode(task.getDataSaveMode());
        if (!skipSchemaPrepare && shouldPrepareSchemaBeforeSync(schemaSaveMode)) {
            prepareTargetSchemas(task, tables, schemaSaveMode);
        }
        String seatunnelSchemaSaveMode = shouldPrepareSchemaBeforeSync(schemaSaveMode) ? "IGNORE" : schemaSaveMode;

        StringBuilder cfg = new StringBuilder();
        if ("batch".equals(task.getSyncType())) {
            // 表多时降低并行度，避免 StarRocks 版本积压
            String parallelism = tables.size() > 10 ? "1" : "2";
            cfg.append("env {\n  parallelism = " + parallelism + "\n  job.mode = \"BATCH\"\n}\n\nsource {\n");
            for (int i = 0; i < tables.size(); i++) {
                Map<String, Object> t = tables.get(i);
                String pluginOutput = multiTable ? String.format("    plugin_output = \"table_%d\"\n", i) : "";
                String colList = buildCompatibleColumnList(task.getSourceId(),
                        (String) t.get("sourceDatabase"), (String) t.get("sourceTable"),
                        targetDs, (String) t.get("targetDatabase"), (String) t.get("targetTable"));
                // 构建 query
                String query = "SELECT " + colList + " FROM " + quoteIdentifier(String.valueOf(t.get("sourceDatabase")))
                        + "." + quoteIdentifier(String.valueOf(t.get("sourceTable")));
                if (hasIncrWhere) {
                    query += " WHERE " + incrWhere.trim();
                }
                cfg.append(String.format("  Jdbc {\n%s    url = %s\n    driver = \"com.mysql.cj.jdbc.Driver\"\n    user = %s\n    password = %s\n    int_type_narrowing = false\n    query = %s\n  }\n",
                        pluginOutput, hoconString(sourceUrl), hoconString(sourceDs.getUsername()), hoconString(sourcePwd), hoconString(query)));
            }
            cfg.append("}\n\nsink {\n");
            // StarRocks sink 公共参数（减少版本积压）
            String srSinkCommon = "    batch_max_rows = 10240\n    batch_max_bytes = 52428800\n    max_retries = 5\n    retry_backoff_multiplier_ms = 200\n    max_retry_backoff_ms = 60000\n    enable_upsert_delete = true\n"
                    + "    schema_save_mode = " + seatunnelSchemaSaveMode + "\n    data_save_mode = " + dataSaveMode + "\n"
                    + "    starrocks.config = {\n      format = \"JSON\"\n      strip_outer_array = true\n    }\n";
            for (int i = 0; i < tables.size(); i++) {
                Map<String, Object> t = tables.get(i);
                String pluginInput = multiTable ? String.format("    plugin_input = \"table_%d\"\n", i) : "";
                cfg.append(String.format("  StarRocks {\n%s    base-url = %s\n    nodeUrls = [%s]\n    username = %s\n    password = %s\n    database = %s\n    table = %s\n%s  }\n",
                        pluginInput, hoconString(srBaseUrl), srNodeUrls, hoconString(targetDs.getUsername()), hoconString(targetPwd),
                        hoconString(String.valueOf(t.get("targetDatabase"))), hoconString(String.valueOf(t.get("targetTable"))), srSinkCommon));
            }
            cfg.append("}\n");
        } else {
            // 实时任务：从 rtConfig 取可配置参数，默认有值
            int rtp = getIntParam(rtConfig, "rtParallelism", 2);
            String smode = getStrParam(rtConfig, "rtStartupMode", "initial");
            String sTimestamp = getStrParam(rtConfig, "rtStartupTimestamp", "");
            int cpSec = getIntParam(rtConfig, "rtCheckpointSec", 10);
            int bmr = getIntParam(rtConfig, "rtBatchMaxRows", 10240);
            int bmb = getIntParam(rtConfig, "rtBatchMaxBytes", 52428800);
            String ssm = normalizeSchemaSaveMode(getStrParam(rtConfig, "rtSchemaSaveMode", schemaSaveMode));
            String dsm = normalizeDataSaveMode(getStrParam(rtConfig, "rtDataSaveMode", dataSaveMode));
            String seatunnelSsm = shouldPrepareSchemaBeforeSync(ssm) ? "IGNORE" : ssm;

            cfg.append("env {\n  parallelism = " + rtp + "\n  job.mode = \"STREAMING\"\n  checkpoint.interval = " + (cpSec * 1000) + "\n}\n\nsource {\n");
            
            // 合并所有表到一个 MySQL-CDC source
            String startupLine;
            if ("timestamp".equals(smode) && !sTimestamp.isEmpty()) {
                startupLine = "    startup.mode = \"timestamp\"\n    startup.timestamp = \"" + sTimestamp + "\"\n";
            } else {
                startupLine = "    startup.mode = \"" + smode + "\"\n";
            }
            
            // 构建 table-list
            StringBuilder tableList = new StringBuilder();
            boolean renameRealtimeTables = false;
            Set<String> realtimeTargetDatabases = new LinkedHashSet<>();
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) tableList.append(", ");
                Map<String, Object> t = tables.get(i);
                tableList.append("\"").append(t.get("sourceDatabase")).append(".").append(t.get("sourceTable")).append("\"");
                realtimeTargetDatabases.add(String.valueOf(t.get("targetDatabase")));
                if (!Objects.equals(String.valueOf(t.get("sourceTable")), String.valueOf(t.get("targetTable")))) {
                    renameRealtimeTables = true;
                }
            }

            if (realtimeTargetDatabases.size() > 1) {
                throw new RuntimeException("实时同步暂不支持一个任务写入多个目标库，请拆分为多个实时任务。");
            }

            String sourceOutput = renameRealtimeTables ? "    plugin_output = \"mysql_cdc_source\"\n" : "";
            cfg.append(String.format("  MySQL-CDC {\n%s%s    server-id = \"5656-5660\"\n    username = %s\n    password = %s\n    base-url = %s\n    table-names = [%s]\n    snapshot.split.size = 16000\n    snapshot.fetch.size = 5000\n  }\n",
                    sourceOutput, startupLine, hoconString(sourceDs.getUsername()), hoconString(sourcePwd), hoconString(sourceUrl), tableList.toString()));

            String srSinkCommon = "    batch_max_rows = " + bmr + "\n    batch_max_bytes = " + bmb
                    + "\n    max_retries = 5\n    retry_backoff_multiplier_ms = 200\n    max_retry_backoff_ms = 60000\n    enable_upsert_delete = true\n"
                    + "    schema_save_mode = " + seatunnelSsm + "\n    data_save_mode = " + dsm + "\n"
                    + "    starrocks.config = {\n      format = \"JSON\"\n      strip_outer_array = true\n    }\n";
            cfg.append("}\n\n");

            String sinkInput = "";
            if (renameRealtimeTables) {
                cfg.append("transform {\n  TableRename {\n    plugin_input = \"mysql_cdc_source\"\n    plugin_output = \"renamed_cdc_tables\"\n    replacements_with_regex = [\n");
                for (int i = 0; i < tables.size(); i++) {
                    Map<String, Object> t = tables.get(i);
                    if (i > 0) cfg.append(",\n");
                    cfg.append("      {\n")
                            .append("        replace_from = ").append(hoconString("^" + Pattern.quote(String.valueOf(t.get("sourceTable"))) + "$")).append("\n")
                            .append("        replace_to = ").append(hoconString(String.valueOf(t.get("targetTable")))).append("\n")
                            .append("      }");
                }
                cfg.append("\n    ]\n  }\n}\n\n");
                sinkInput = "    plugin_input = \"renamed_cdc_tables\"\n";
            }

            cfg.append("sink {\n");
            // 单个 StarRocks sink，使用动态路由
            cfg.append(String.format("  StarRocks {\n%s    base-url = %s\n    nodeUrls = [%s]\n    username = %s\n    password = %s\n    database = %s\n    table = \"${table_name}\"\n%s  }\n",
                    sinkInput, hoconString(srBaseUrl), srNodeUrls, hoconString(targetDs.getUsername()), hoconString(targetPwd),
                    hoconString(String.valueOf(tables.get(0).get("targetDatabase"))), srSinkCommon));
            cfg.append("}\n");
        }
        return cfg.toString();
    }

    private boolean shouldPrepareSchemaBeforeSync(String schemaSaveMode) {
        return "CREATE_SCHEMA_WHEN_NOT_EXIST".equals(schemaSaveMode) || "RECREATE_SCHEMA".equals(schemaSaveMode);
    }

    private void prepareTargetSchemas(SyncTask task, List<Map<String, Object>> tables, String schemaSaveMode) {
        Datasource targetDs = datasourceService.getById(task.getTargetId());
        if (targetDs == null) throw new RuntimeException("目标数据源不存在");

        String[] hosts = targetDs.getHost().split(",");
        String host = hosts[0].trim();
        String url = String.format("jdbc:mysql://%s:%d?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true",
                host, targetDs.getPort());

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, targetDs.getUsername(), targetDs.getPassword());
             java.sql.Statement stmt = conn.createStatement()) {
            for (Map<String, Object> t : tables) {
                String sourceDatabase = String.valueOf(t.get("sourceDatabase"));
                String sourceTable = String.valueOf(t.get("sourceTable"));
                String targetDatabase = String.valueOf(t.get("targetDatabase"));
                String targetTable = String.valueOf(t.get("targetTable"));
                List<Map<String, String>> sourceColumns = metadataService.getColumns(
                        task.getSourceId(), sourceDatabase, sourceTable);
                ensureTargetDatabaseSelectedAndExists(conn, targetDatabase);

                if ("RECREATE_SCHEMA".equals(schemaSaveMode)) {
                    stmt.execute("DROP TABLE IF EXISTS " + quoteIdentifier(targetDatabase) + "." + quoteIdentifier(targetTable));
                    stmt.execute(metadataService.generateDdlFromColumns(sourceColumns, targetDatabase, targetTable));
                    continue;
                }

                if (!targetTableExists(conn, targetDatabase, targetTable)) {
                    stmt.execute(metadataService.generateDdlFromColumns(sourceColumns, targetDatabase, targetTable));
                    continue;
                }
                addMissingTargetColumns(conn, stmt, task, sourceDatabase, sourceTable, targetDatabase, targetTable);
            }
        } catch (Exception e) {
            throw new RuntimeException("自动补齐目标表字段失败: " + e.getMessage(), e);
        }
    }

    private void ensureTargetDatabaseSelectedAndExists(java.sql.Connection conn, String database) throws Exception {
        if (database == null || database.trim().isEmpty() || "null".equalsIgnoreCase(database.trim())) {
            throw new RuntimeException("Target database is empty. Please select an existing target database in task config.");
        }
        try (java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet ignored = stmt.executeQuery("SHOW TABLES FROM "
                     + quoteIdentifier(database) + " LIKE '__datasync_database_probe__'")) {
            return;
        } catch (Exception e) {
            if (isDatabaseNotFound(e)) {
                throw new RuntimeException("Target database " + database
                        + " does not exist. Please create it manually and select it in task config.", e);
            }
            throw e;
        }
    }

    private List<String> getColumnNames(Long datasourceId, String database, String table) {
        List<Map<String, String>> columns = metadataService.getColumns(datasourceId, database, table);
        return columns.stream()
                .map(c -> c.get("columnName"))
                .collect(Collectors.toList());
    }

    private String buildCompatibleColumnList(Long sourceId, String sourceDatabase, String sourceTable,
                                             Datasource targetDs, String targetDatabase, String targetTable) {
        List<String> sourceColumns = getColumnNames(sourceId, sourceDatabase, sourceTable);
        if (sourceColumns.isEmpty()) return "*";
        List<String> targetColumns = getColumnNamesIfTableExists(targetDs, targetDatabase, targetTable);
        if (targetColumns != null && !targetColumns.isEmpty()) {
            // 目标表已存在时校验字段完整性：缺失则明确报错，避免 SELECT 交集静默丢字段
            // （默认 CREATE_SCHEMA_WHEN_NOT_EXIST/RECREATE_SCHEMA 模式下 prepareTargetSchemas 已先补齐，此处不会触发）
            List<String> missing = sourceColumns.stream()
                    .filter(c -> !targetColumns.contains(c))
                    .collect(Collectors.toList());
            if (!missing.isEmpty()) {
                throw new RuntimeException("目标表 " + targetDatabase + "." + targetTable
                        + " 缺少源表字段: " + String.join(", ", missing)
                        + "。请先执行「同步表结构(DDL)」，或使用 schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST / RECREATE_SCHEMA 让系统自动补齐字段");
            }
        }
        // 始终选择源表全部字段，保证字段完整同步
        return sourceColumns.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
    }

    private List<String> getColumnNamesIfTableExists(Datasource ds, String database, String table) {
        String[] hosts = ds.getHost().split(",");
        String host = hosts[0].trim();
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=Asia/Shanghai",
                host, ds.getPort(), database);
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, ds.getUsername(), ds.getPassword());
             java.sql.Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SHOW FULL COLUMNS FROM " + quoteIdentifier(table))) {
            List<String> columns = new ArrayList<>();
            while (rs.next()) {
                columns.add(rs.getString("Field"));
            }
            return columns;
        } catch (Exception e) {
            String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (message.contains("doesn't exist") || message.contains("unknown table")
                    || message.contains("unknown database") || message.contains("table not found")) {
                return null;
            }
            throw new RuntimeException("读取目标表结构失败: " + database + "." + table + "，" + e.getMessage(), e);
        }
    }

    private String quoteIdentifier(String identifier) {
        if (identifier == null) return "``";
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String hoconString(String value) {
        if (value == null) return "\"\"";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private String escapeComment(String comment) {
        if (comment == null) return "";
        return comment.replace("\\", "\\\\").replace("'", "\\'");
    }

    /**
     * 获取源表的列名列表
     */
    private static int getIntParam(Map<String, Object> map, String key, int defaultVal) {
        if (map == null || !map.containsKey(key)) return defaultVal;
        Object v = map.get(key);
        if (v instanceof Number) return ((Number) v).intValue();
        try { return Integer.parseInt(String.valueOf(v)); } catch (Exception e) { return defaultVal; }
    }

    private static String getStrParam(Map<String, Object> map, String key, String defaultVal) {
        if (map == null || !map.containsKey(key)) return defaultVal;
        Object v = map.get(key);
        return v != null ? String.valueOf(v) : defaultVal;
    }

}
