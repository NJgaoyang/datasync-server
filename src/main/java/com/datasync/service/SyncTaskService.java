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

    public List<SyncTask> listTasks() {
        return taskRepository.findAll();
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
        regenerateConfig(taskId);
        schedulerService.executeTask(taskId);
    }

    /**
     * 重新生成任务的 SeaTunnel 配置（用最新代码逻辑，解决旧配置参数过期问题）
     */
    public void regenerateConfig(Long taskId) {
        SyncTask task = taskRepository.findById(taskId).orElseThrow(() -> new RuntimeException("任务不存在"));
        List<SyncTaskTable> tables = tableRepository.findByTaskId(taskId);
        if (tables.isEmpty()) return;

        // 构建 tables 列表
        List<Map<String, Object>> tableMaps = tables.stream().map(t -> {
            Map<String, Object> m = new HashMap<>();
            m.put("sourceDatabase", t.getSourceDatabase());
            m.put("sourceTable", t.getSourceTable());
            m.put("targetDatabase", t.getTargetDatabase());
            m.put("targetTable", t.getTargetTable());
            return m;
        }).collect(Collectors.toList());

        // 实时参数使用默认值（实时任务的 rtConfig 未持久化到实体）
        Map<String, Object> rtConfig = new HashMap<>();
        if ("realtime".equals(task.getSyncType())) {
            rtConfig.put("rtParallelism", 2);
            rtConfig.put("rtStartupMode", "initial");
            rtConfig.put("rtCheckpointSec", 5);
            rtConfig.put("rtSchemaSaveMode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
            rtConfig.put("rtDataSaveMode", "APPEND_DATA");
        }

        String config = buildSeaTunnelConfig(task, tableMaps, null, rtConfig);
        task.setSeatunnelConfig(config);
        taskRepository.save(task);
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
                    String execDdl = ddl;
                    stmt.execute(execDdl);
                    result.put("success", true);
                    result.put("message", dropIfExists ? "已删除并重建" : "建表成功");
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

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables) {
        return buildSeaTunnelConfig(task, tables, null, new HashMap<>());
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables, Map<String, String> colListCache) {
        return buildSeaTunnelConfig(task, tables, colListCache, new HashMap<>());
    }

    private String buildSeaTunnelConfig(SyncTask task, List<Map<String, Object>> tables, Map<String, String> colListCache, Map<String, Object> rtConfig) {
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

        StringBuilder cfg = new StringBuilder();
        if ("batch".equals(task.getSyncType())) {
            // 表多时降低并行度，避免 StarRocks 版本积压
            String parallelism = tables.size() > 10 ? "1" : "2";
            cfg.append("env {\n  execution.parallelism = " + parallelism + "\n  job.mode = \"BATCH\"\n}\n\nsource {\n");
            for (int i = 0; i < tables.size(); i++) {
                Map<String, Object> t = tables.get(i);
                String pluginOutput = multiTable ? String.format("    plugin_output = \"table_%d\"\n", i) : "";
                String colList;
                if (colListCache != null) {
                    String key = t.get("sourceDatabase") + "." + t.get("sourceTable");
                    colList = colListCache.getOrDefault(key, "*");
                } else {
                    colList = buildColumnList(task.getSourceId(),
                            (String) t.get("sourceDatabase"), (String) t.get("sourceTable"));
                }
                // 构建 query
                String query = "SELECT " + colList + " FROM " + t.get("sourceDatabase") + "." + t.get("sourceTable");
                if (hasIncrWhere) {
                    query += " WHERE " + incrWhere.trim();
                }
                cfg.append(String.format("  Jdbc {\n%s    url = \"%s\"\n    driver = \"com.mysql.cj.jdbc.Driver\"\n    username = \"%s\"\n    password = \"%s\"\n    query = \"%s\"\n  }\n",
                        pluginOutput, sourceUrl, sourceDs.getUsername(), sourcePwd, query));
            }
            cfg.append("}\n\nsink {\n");
            // StarRocks sink 公共参数（减少版本积压）
            String srSinkCommon = "    batch_max_rows = 10240\n    batch_max_bytes = 52428800\n    batch_interval_ms = 10000\n    max_retries = 5\n    retry_backoff_multiplier_ms = 200\n    max_retry_backoff_ms = 60000\n    enable_upsert_delete = true\n";
            for (int i = 0; i < tables.size(); i++) {
                Map<String, Object> t = tables.get(i);
                String pluginInput = multiTable ? String.format("    plugin_input = \"table_%d\"\n", i) : "";
                cfg.append(String.format("  StarRocks {\n%s    base-url = \"%s\"\n    nodeUrls = [%s]\n    username = \"%s\"\n    password = \"%s\"\n    database = \"%s\"\n    table = \"%s\"\n%s  }\n",
                        pluginInput, srBaseUrl, srNodeUrls, targetDs.getUsername(), targetPwd,
                        t.get("targetDatabase"), t.get("targetTable"), srSinkCommon));
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
            String ssm = getStrParam(rtConfig, "rtSchemaSaveMode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
            String dsm = getStrParam(rtConfig, "rtDataSaveMode", "APPEND_DATA");

            cfg.append("env {\n  execution.parallelism = " + rtp + "\n  job.mode = \"STREAMING\"\n  checkpoint.interval = " + (cpSec * 1000) + "\n}\n\nsource {\n");
            
            // 合并所有表到一个 MySQL-CDC source
            String startupLine;
            if ("timestamp".equals(smode) && !sTimestamp.isEmpty()) {
                startupLine = "    startup.mode = \"timestamp\"\n    startup.timestamp = \"" + sTimestamp + "\"\n";
            } else {
                startupLine = "    startup.mode = \"" + smode + "\"\n";
            }
            
            // 构建 table-list
            StringBuilder tableList = new StringBuilder();
            for (int i = 0; i < tables.size(); i++) {
                if (i > 0) tableList.append(", ");
                Map<String, Object> t = tables.get(i);
                tableList.append("\"").append(t.get("sourceDatabase")).append(".").append(t.get("sourceTable")).append("\"");
            }
            
            cfg.append(String.format("  MySQL-CDC {\n%s    server-id = \"5656-5660\"\n    username = \"%s\"\n    password = \"%s\"\n    base-url = \"%s\"\n    table-names = [%s]\n    snapshot.split.size = 16000\n    snapshot.fetch.size = 5000\n  }\n",
                    startupLine, sourceDs.getUsername(), sourcePwd, sourceUrl, tableList.toString()));

            String srSinkCommon = "    batch_max_rows = " + bmr + "\n    batch_max_bytes = " + bmb
                    + "\n    batch_interval_ms = 10000\n    max_retries = 5\n    retry_backoff_multiplier_ms = 200\n    max_retry_backoff_ms = 60000\n    enable_upsert_delete = true\n"
                    + "    schema_save_mode = " + ssm + "\n    data_save_mode = " + dsm + "\n";
            cfg.append("}\n\nsink {\n");
            
            // 单个 StarRocks sink，使用动态路由
            cfg.append(String.format("  StarRocks {\n    base-url = \"%s\"\n    nodeUrls = [%s]\n    username = \"%s\"\n    password = \"%s\"\n    database = \"%s\"\n    table = \"${table_name}\"\n%s  }\n",
                    srBaseUrl, srNodeUrls, targetDs.getUsername(), targetPwd,
                    tables.get(0).get("targetDatabase"), srSinkCommon));
            cfg.append("}\n");
        }
        return cfg.toString();
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

    private String buildColumnList(Long sourceId, String database, String table) {
        try {
            List<Map<String, String>> columns = metadataService.getColumns(sourceId, database, table);
            List<String> colNames = columns.stream()
                    .map(c -> c.get("columnName"))
                    .collect(Collectors.toList());
            if (colNames.isEmpty()) return "*";
            return colNames.stream()
                    .map(cn -> "`" + cn + "`")
                    .collect(Collectors.joining(", "));
        } catch (Exception e) {
            System.out.println("[buildColumnList] fallback to * for " + database + "." + table + ": " + e.getMessage());
            return "*";
        }
    }
}
