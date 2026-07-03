package com.datasync.controller;

import com.datasync.entity.SyncTask;
import com.datasync.entity.SyncTaskTable;
import com.datasync.entity.TaskExecution;
import com.datasync.service.SyncTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.datasync.repository.TaskExecutionRepository;
import java.util.*;

@RestController
@RequestMapping("/api/task")
public class TaskController {
    @Autowired
    private SyncTaskService taskService;

    @Autowired
    private TaskExecutionRepository executionRepository;

    @PostMapping("/create")
    public SyncTask create(@RequestBody Map<String, Object> params) {
        return taskService.createTask(params);
    }

    @PutMapping("/update")
    public SyncTask update(@RequestBody Map<String, Object> params) {
        return taskService.updateTask(params);
    }

    @PutMapping("/cron")
    public SyncTask setCron(@RequestBody Map<String, Object> params) {
        return taskService.setCron(params);
    }

    @GetMapping("/list")
    public List<Map<String, Object>> list() {
        return taskService.listTaskSummaries();
    }

    @GetMapping("/stats/dashboard")
    public Map<String, Object> getDashboardStats() {
        return taskService.getDashboardStats();
    }

    @GetMapping("/search")
    public List<SyncTask> search(@RequestParam(required = false) String keyword) {
        return taskService.searchTasks(keyword);
    }

    @GetMapping("/tables/synced")
    public List<Map<String, Object>> listSyncedTables() {
        return taskService.listSyncedTables();
    }

    @GetMapping("/{id}")
    public SyncTask getById(@PathVariable Long id) {
        return taskService.getTask(id);
    }

    @GetMapping("/{id}/tables")
    public List<SyncTaskTable> getTaskTables(@PathVariable Long id) {
        return taskService.getTaskTables(id);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable Long id) {
        taskService.deleteTask(id);
    }

    @PutMapping("/{id}")
    public SyncTask updateTask(@PathVariable Long id, @RequestBody Map<String, Object> params) {
        return taskService.updateTask(id, params);
    }

    /** 上下线切换 */
    @PostMapping("/{id}/toggle")
    public SyncTask toggleEnabled(@PathVariable Long id, @RequestBody Map<String, Object> params) {
        boolean enabled = Boolean.parseBoolean(params.getOrDefault("enabled", "false").toString());
        return taskService.toggleEnabled(id, enabled);
    }

    /** 定时调度开关切换 */
    @PostMapping("/{id}/toggle-schedule")
    public SyncTask toggleSchedule(@PathVariable Long id, @RequestBody Map<String, Object> params) {
        boolean scheduleEnabled = Boolean.parseBoolean(params.getOrDefault("scheduleEnabled", "false").toString());
        return taskService.toggleSchedule(id, scheduleEnabled);
    }

    /** 同步建表：在目标 StarRocks 执行 DDL */
    @PostMapping("/{id}/sync-ddl")
    public List<Map<String, Object>> syncDdl(@PathVariable Long id, @RequestBody Map<String, Object> params) {
        boolean dropIfExists = Boolean.parseBoolean(params.getOrDefault("dropIfExists", "false").toString());
        return taskService.syncDdl(id, dropIfExists);
    }

    /** 手动执行一次 */
    @PostMapping("/{id}/execute")
    public void execute(@PathVariable Long id) {
        taskService.executeTask(id);
    }

    /** 停止运行 */
    @PostMapping("/{id}/stop")
    public void stop(@PathVariable Long id) {
        taskService.stopTask(id);
    }

    /** 获取任务状态概览 */
    @GetMapping("/{id}/status")
    public Map<String, Object> getStatus(@PathVariable Long id) {
        return taskService.getTaskStatus(id);
    }

    /** 获取执行历史 */
    @GetMapping("/{id}/executions")
    public List<TaskExecution> getExecutions(@PathVariable Long id,
                                             @RequestParam(defaultValue = "20") int limit) {
        return taskService.getExecutionHistory(id, limit);
    }

    /** 获取单次执行详情（含日志） */
    @GetMapping("/execution/{execId}")
    public TaskExecution getExecution(@PathVariable Long execId) {
        return taskService.getExecution(execId);
    }

    /** 任务日志分页查询（关联任务名，支持日期+状态筛选） */
    @GetMapping("/executions/page")
    public Map<String, Object> getExecutionsPage(@RequestParam(defaultValue = "") String keyword,
                                                  @RequestParam(defaultValue = "0") int page,
                                                  @RequestParam(defaultValue = "15") int size,
                                                  @RequestParam(required = false) String startDate,
                                                  @RequestParam(required = false) String endDate,
                                                  @RequestParam(required = false) String status) {
        return taskService.getExecutionsPage(keyword, page, size, startDate, endDate, status);
    }

    /** 批量删除任务执行记录（管理员） */
    @DeleteMapping("/executions/batch")
    public Map<String, Object> deleteExecutions(@RequestBody Map<String, Object> params) {
        @SuppressWarnings("unchecked")
        List<Number> rawIds = (List<Number>) params.get("ids");
        if (rawIds == null || rawIds.isEmpty()) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "请选择要删除的记录");
            return error;
        }
        List<Long> ids = rawIds.stream().map(Number::longValue).collect(java.util.stream.Collectors.toList());
        int count = taskService.deleteExecutions(ids);
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", "成功删除 " + count + " 条记录");
        result.put("count", count);
        return result;
    }

    /** 执行统计：当日汇总 + 日期范围内按天趋势 */
    @GetMapping("/stats/executions")
    public Map<String, Object> getExecutionStats(@RequestParam String startDate,
                                                  @RequestParam String endDate) {
        Map<String, Object> result = new HashMap<>();

        // 当日统计
        List<Object[]> todayStats = executionRepository.getTodayStats();
        long todaySuccess = 0, todayFailed = 0, todayTotal = 0;
        for (Object[] row : todayStats) {
            String status = (String) row[0];
            long cnt = ((Number) row[1]).longValue();
            if ("SUCCESS".equals(status)) todaySuccess = cnt;
            else if ("FAILED".equals(status)) todayFailed = cnt;
            todayTotal += cnt;
        }
        Map<String, Long> today = new LinkedHashMap<>();
        today.put("success", todaySuccess);
        today.put("failed", todayFailed);
        today.put("total", todayTotal);
        result.put("today", today);

        // 日期范围内的每日趋势
        List<Object[]> stats = executionRepository.getExecutionStatsByDate(startDate + " 00:00:00", endDate + " 23:59:59");
        Map<String, Map<String, Long>> dailyMap = new LinkedHashMap<>();
        for (Object[] row : stats) {
            String date = row[0].toString();
            String status = (String) row[1];
            long cnt = ((Number) row[2]).longValue();
            dailyMap.computeIfAbsent(date, k -> {
                Map<String, Long> m = new LinkedHashMap<>();
                m.put("success", 0L);
                m.put("failed", 0L);
                return m;
            });
            if ("SUCCESS".equals(status)) {
                dailyMap.get(date).put("success", cnt);
            } else if ("FAILED".equals(status)) {
                dailyMap.get(date).put("failed", cnt);
            }
        }
        List<Map<String, Object>> chartData = new ArrayList<>();
        for (Map.Entry<String, Map<String, Long>> entry : dailyMap.entrySet()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("date", entry.getKey());
            item.put("success", entry.getValue().get("success"));
            item.put("failed", entry.getValue().get("failed"));
            chartData.add(item);
        }
        result.put("chartData", chartData);
        return result;
    }
}
