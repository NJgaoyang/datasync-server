package com.datasync.service;

import com.datasync.entity.ClusterClient;
import com.datasync.entity.SyncTask;
import com.datasync.entity.TaskExecution;
import com.datasync.repository.ClusterClientRepository;
import com.datasync.repository.SyncTaskRepository;
import com.datasync.repository.TaskExecutionRepository;
import com.datasync.util.AesUtil;
import com.jcraft.jsch.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class SchedulerService {

    @Autowired
    private SyncTaskRepository taskRepository;

    @Autowired
    private TaskExecutionRepository executionRepository;

    @Autowired
    private ClusterClientRepository clusterClientRepository;

    @Autowired(required = false)
    private AlertService alertService;

    @Value("${scheduler.thread-pool-size:5}")
    private int threadPoolSize;

    @Value("${datasync.secret-key:DataSync@2026!Key}")
    private String secretKey;

    @Value("${seatunnel.home:/data/software/apache-seatunnel-2.3.13}")
    private String seatunnelHomeConfig;

    private ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<Long, Future<?>> runningTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, StringBuffer> runningLogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        scheduler = Executors.newScheduledThreadPool(threadPoolSize);
        // 延迟3秒后恢复已有的定时调度（等数据库就绪）
        scheduler.schedule(this::initScheduledTasks, 3, TimeUnit.SECONDS);
        // 清理服务重启前残留的 RUNNING 状态（内存中 Future 已丢失，无法再停止）
        scheduler.schedule(this::cleanStaleRunningTasks, 2, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        if (scheduler != null) scheduler.shutdownNow();
    }

    /**
     * 清理服务重启后残留的 RUNNING 状态任务（内存中 Future 已丢失导致无法停止）
     */
    private void cleanStaleRunningTasks() {
        try {
            List<SyncTask> running = taskRepository.findByLastExecStatus("RUNNING");
            for (SyncTask task : running) {
                task.setLastExecStatus("FAILED");
                task.setStatus("GENERATED");
                task.setLastExecTime(LocalDateTime.now());
                taskRepository.save(task);
            }
            // 清理残留的 RUNNING/PENDING 执行记录
            List<TaskExecution> staleExecs = new ArrayList<>(executionRepository.findByStatus("RUNNING"));
            staleExecs.addAll(executionRepository.findByStatus("PENDING"));
            for (TaskExecution exec : staleExecs) {
                exec.setStatus("FAILED");
                exec.setFinishedAt(LocalDateTime.now());
                if (exec.getLogText() == null) {
                    exec.setLogText("[WARN] 服务重启,任务被中断");
                } else {
                    exec.setLogText(exec.getLogText() + "\n[WARN] 服务重启,任务被中断");
                }
                executionRepository.save(exec);
            }
            if (!running.isEmpty()) {
                System.out.println("[DataSync] 清理了 " + running.size() + " 个残留 RUNNING 状态的任务");
            }
        } catch (Exception e) {
            System.out.println("[DataSync] 清理残留任务状态失败: " + e.getMessage());
        }
    }

    /**
     * 启动时从数据库恢复所有启用调度的在线任务
     */
    private void initScheduledTasks() {
        List<SyncTask> tasks = taskRepository.findAll();
        for (SyncTask task : tasks) {
            if (Boolean.TRUE.equals(task.getEnabled())
                    && Boolean.TRUE.equals(task.getScheduleEnabled())
                    && task.getCronExpression() != null
                    && !task.getCronExpression().trim().isEmpty()) {
                scheduleCronTask(task);
            }
        }
    }

    /**
     * 外部触发执行一个任务（手动执行）
     */
    public void executeTask(Long taskId) {
        SyncTask task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));
        if (!Boolean.TRUE.equals(task.getEnabled())) {
            throw new RuntimeException("任务处于离线状态，请先上线后再执行");
        }
        executeTaskInternal(task);
    }

    private void executeTaskInternal(SyncTask task) {
        // 如果已有运行中的实例，不再启动
        if (runningTasks.containsKey(task.getId())) {
            throw new RuntimeException("任务正在运行中，请等待完成");
        }

        // 创建执行记录
        TaskExecution execution = new TaskExecution();
        execution.setTaskId(task.getId());
        execution.setStatus("PENDING");
        execution.setLogText("");
        executionRepository.save(execution);

        // 更新任务状态
        task.setLastExecStatus("RUNNING");
        task.setLastExecTime(LocalDateTime.now());
        task.setLastExecDuration("运行中...");
        task.setLastExecRows(null);
        task.setLastExecQps(null);
        task.setStatus("RUNNING");
        taskRepository.save(task);

        Long execId = execution.getId();
        StringBuffer logBuf = new StringBuffer();
        runningLogs.put(task.getId(), logBuf);

        // 提交到线程池执行
        Future<?> future = scheduler.submit(() -> {
            try {
                runSeaTunnelJob(task, execId, logBuf);

                execution.setStatus("SUCCESS");
                execution.setFinishedAt(LocalDateTime.now());
                execution.setLogText(logBuf.toString());
                executionRepository.save(execution);

                calcExecMetricsFinal(task, execution, logBuf.toString());
                task.setLastExecStatus("SUCCESS");
                task.setLastExecTime(LocalDateTime.now());
                task.setStatus("GENERATED");
                taskRepository.save(task);

                // 发送成功告警
                sendAlertIfNeeded(task, "SUCCESS", "任务执行成功");
            } catch (Exception e) {
                logBuf.append("\n[ERROR] ").append(e.getMessage()).append("\n");
                execution.setStatus("FAILED");
                execution.setFinishedAt(LocalDateTime.now());
                execution.setLogText(logBuf.toString());
                executionRepository.save(execution);

                calcExecMetricsFinal(task, execution, logBuf.toString());
                if (task.getLastExecRows() == null) {
                    task.setLastExecDuration(null);
                    task.setLastExecQps(null);
                }
                task.setLastExecStatus("FAILED");
                task.setLastExecTime(LocalDateTime.now());
                task.setStatus("GENERATED");
                taskRepository.save(task);

                // 发送失败告警
                sendAlertIfNeeded(task, "FAILED", e.getMessage() != null ? e.getMessage() : "未知错误");
            } finally {
                runningTasks.remove(task.getId());
                runningLogs.remove(task.getId());
            }
        });
        runningTasks.put(task.getId(), future);
    }

    /**
     * 实际运行 SeaTunnel 任务
     */
    private void runSeaTunnelJob(SyncTask task, Long execId, StringBuffer logBuf) throws Exception {
        String config = task.getSeatunnelConfig();
        if (config == null || config.isEmpty()) {
            throw new RuntimeException("SeaTunnel 配置为空，无法执行");
        }

        // 解密配置中的密码（AES:xxx → 明文）
        config = decryptConfigPasswords(config);

        boolean isCluster = "cluster".equals(task.getDeployMode());

        // 集群模式 + 有集群客户端配置 → 尝试 SSH 远程执行
        if (isCluster && task.getClusterId() != null) {
            ClusterClient cluster = clusterClientRepository.findById(task.getClusterId()).orElse(null);
            if (cluster != null && cluster.getSshUser() != null && !cluster.getSshUser().isEmpty()) {
                runViaSsh(task, config, cluster, execId, logBuf);
                return;
            }
        }

        // 本地执行模式（客户端模式，或集群模式但无SSH配置时回退）
        String configFile = System.getProperty("java.io.tmpdir") + "/seatunnel_job_" + task.getId() + ".conf";
        try (FileWriter fw = new FileWriter(configFile)) {
            fw.write(config);
        }

        String seatunnelHome = System.getenv("SEATUNNEL_HOME");
        if (seatunnelHome == null || seatunnelHome.isEmpty()) {
            seatunnelHome = seatunnelHomeConfig;
        }

        boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
        String script = seatunnelHome + "/bin/seatunnel." + (isWindows ? "bat" : "sh");

        List<String> command = new ArrayList<>();
        command.add(script);
        command.add("--config");
        command.add(configFile);
        if (isCluster) {
            command.add("--master");
            command.add("cluster");
        }

        String mode = isCluster ? "[集群模式-本地]" : "[客户端模式]";
        logBuf.append("[").append(LocalDateTime.now()).append("] ").append(mode).append(" 启动命令: ").append(String.join(" ", command)).append("\n");

        execLocal(command, seatunnelHome, execId, logBuf);
    }

    /**
     * 通过 SSH 远程执行 SeaTunnel
     */
    private void runViaSsh(SyncTask task, String config, ClusterClient cluster, Long execId, StringBuffer logBuf) throws Exception {
        String remoteConfigPath = "/tmp/seatunnel_job_" + task.getId() + ".conf";
        String stHome = cluster.getSeatunnelHome();
        if (stHome == null || stHome.isEmpty()) {
            stHome = seatunnelHomeConfig;
        }

        JSch jsch = new JSch();
        Session session = jsch.getSession(cluster.getSshUser(), cluster.getHost(), cluster.getSshPort() != null ? cluster.getSshPort() : 22);

        // SSH 密码解密
        String sshPwd = cluster.getSshPassword();
        if (sshPwd != null && sshPwd.startsWith("AES:")) {
            sshPwd = AesUtil.decrypt(sshPwd, secretKey);
        }
        final String pwd2 = sshPwd;
        if (pwd2 == null || pwd2.isEmpty()) {
            throw new RuntimeException("SSH密码为空，请编辑集群客户端重新填写密码");
        }
        session.setPassword(pwd2);
        session.setUserInfo(new com.datasync.service.SshUserInfo(pwd2));
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "keyboard-interactive,password");
        session.setConfig("kex", "diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group-exchange-sha256,ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521,diffie-hellman-group14-sha256");
        session.connect(10000);

        logBuf.append("[").append(LocalDateTime.now()).append("] [SSH] 连接到 ").append(cluster.getSshUser()).append("@").append(cluster.getHost()).append(":").append(cluster.getSshPort()).append("\n");

        try {
            // 1. SCP 上传配置文件
            ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(5000);
            try (InputStream in = new java.io.ByteArrayInputStream(config.getBytes("UTF-8"))) {
                sftp.put(in, remoteConfigPath);
            }
            sftp.disconnect();
            logBuf.append("[").append(LocalDateTime.now()).append("] [SSH] 配置文件已上传: ").append(remoteConfigPath).append("\n");

            // 2. SSH 执行 SeaTunnel
            String command = stHome + "/bin/seatunnel.sh --config " + remoteConfigPath + " --master cluster 2>&1";
            logBuf.append("[").append(LocalDateTime.now()).append("] [SSH-集群] 远程命令: ").append(command).append("\n");

            ChannelExec exec = (ChannelExec) session.openChannel("exec");
            exec.setPty(true); // 分配伪终端，确保输出完整
            exec.setCommand(command);

            final ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
            exec.setOutputStream(capturedOut);
            exec.setErrStream(capturedOut);
            exec.connect();

            // 等待命令执行完成，同时定期存DB（长任务也能看到中间日志）
            long lastSave2 = System.currentTimeMillis();
            while (!exec.isClosed()) {
                Thread.sleep(500);
                // 每10秒存一次中间日志
                long now = System.currentTimeMillis();
                if (now - lastSave2 > 10000 && capturedOut.size() > 0) {
                    String partial = capturedOut.toString("UTF-8");
                    TaskExecution ex = executionRepository.findById(execId).orElse(null);
                    if (ex != null) {
                        ex.setLogText(partial);
                        executionRepository.save(ex);
                    }
                    lastSave2 = now;
                }
            }
            int exitCode = exec.getExitStatus();
            exec.disconnect();

            // 全部输出写入 logBuf（含指标行）
            String allOutput = capturedOut.toString("UTF-8");
            logBuf.append(allOutput);

            logBuf.append("[").append(LocalDateTime.now()).append("] [SSH] 进程退出码: ").append(exitCode).append("\n");
            if (exitCode != 0) {
                throw new RuntimeException("SeaTunnel 集群任务执行失败，退出码: " + exitCode);
            }
        } finally {
            session.disconnect();
        }
    }

    /**
     * 本地执行 shell 命令
     */
    private void execLocal(List<String> command, String workDir, Long execId, StringBuffer logBuf) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new java.io.File(workDir));
        pb.redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        env.put("TZ", "Asia/Shanghai");
        env.put("SEATUNNEL_HOME", workDir);

        Process process = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logBuf.append("[").append(LocalDateTime.now()).append("] ").append(line).append("\n");
                if (logBuf.length() > 5000) {
                    TaskExecution exec = executionRepository.findById(execId).orElse(null);
                    if (exec != null) {
                        exec.setLogText(logBuf.toString());
                        executionRepository.save(exec);
                    }
                }
            }
        }
        int exitCode = process.waitFor();
        logBuf.append("[").append(LocalDateTime.now()).append("] 进程退出码: ").append(exitCode).append("\n");
        if (exitCode != 0) {
            throw new RuntimeException("SeaTunnel 任务执行失败，退出码: " + exitCode);
        }
    }

    // ========== Cron 定时调度 ==========

    /**
     * 为任务启动 cron 定时调度（external调用前需保证task已持久化）
     */
    public void scheduleCronTask(SyncTask task) {
        cancelCronTask(task.getId()); // 先取消已有的
        if (task.getCronExpression() == null || task.getCronExpression().trim().isEmpty()) return;
        scheduleNext(task);
    }

    /**
     * 递归调度下一次执行
     */
    private void scheduleNext(SyncTask task) {
        long delayMs = computeNextDelay(task.getCronExpression());
        if (delayMs < 0) return;

        ScheduledFuture<?> future = scheduler.schedule(() -> {
            try {
                // 执行前再次检查是否仍然在线且调度开启
                SyncTask current = taskRepository.findById(task.getId()).orElse(null);
                if (current == null || !Boolean.TRUE.equals(current.getEnabled())
                        || !Boolean.TRUE.equals(current.getScheduleEnabled())) {
                    scheduledTasks.remove(task.getId());
                    return;
                }
                executeTaskInternal(current);
            } catch (Exception e) {
                // 执行异常也继续尝试下一次调度
            } finally {
                // 重新调度下一次
                SyncTask refreshed = taskRepository.findById(task.getId()).orElse(null);
                if (refreshed != null && Boolean.TRUE.equals(refreshed.getEnabled())
                        && Boolean.TRUE.equals(refreshed.getScheduleEnabled())
                        && refreshed.getCronExpression() != null
                        && !refreshed.getCronExpression().trim().isEmpty()) {
                    scheduleNext(refreshed);
                } else {
                    scheduledTasks.remove(task.getId());
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS);

        scheduledTasks.put(task.getId(), future);
    }

    /**
     * 取消定时调度
     */
    public void cancelCronTask(Long taskId) {
        ScheduledFuture<?> future = scheduledTasks.remove(taskId);
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
    }

    /**
     * 根据 cron 表达式计算距离下一次执行还需多少毫秒
     * 支持5字段(分 时 日 月 周)或6字段(秒 分 时 日 月 周)
     */
    private long computeNextDelay(String cronExpr) {
        try {
            String[] parts = cronExpr.trim().split("\\s+");
            if (parts.length == 5) {
                // 前面补0秒
                String[] p6 = new String[6];
                p6[0] = "0";
                System.arraycopy(parts, 0, p6, 1, 5);
                parts = p6;
            }
            if (parts.length < 6) return -1;

            int[] mins  = expand(parts[1], 59);
            int[] hrs   = expand(parts[2], 23);
            int[] mons  = expand(parts[4], 12);
            boolean isDay  = !"?".equals(parts[3]);
            boolean isWeek = !"?".equals(parts[5]);
            int[] days  = isDay  ? expand(parts[3], 31) : new int[0];
            int[] weeks = isWeek ? expand(parts[5], 7)  : new int[0];
            if (mins.length == 0 || hrs.length == 0 || mons.length == 0) return -1;

            java.util.Calendar now = java.util.Calendar.getInstance();
            now.set(java.util.Calendar.MILLISECOND, 0);
            now.set(java.util.Calendar.SECOND, 0);
            now.add(java.util.Calendar.MINUTE, 1); // 从下一分钟开始

            int safe = 366 * 24 * 5; // 安全上限
            while (safe-- > 0) {
                int mo = now.get(java.util.Calendar.MONTH) + 1;
                if (!contains(mons, mo)) {
                    int nx = nextInArray(mons, mo);
                    if (nx > 0) now.set(java.util.Calendar.MONTH, nx - 1);
                    else { now.add(java.util.Calendar.YEAR, 1); now.set(java.util.Calendar.MONTH, mons[0] - 1); }
                    now.set(java.util.Calendar.DAY_OF_MONTH, 1);
                    now.set(java.util.Calendar.HOUR_OF_DAY, 0);
                    now.set(java.util.Calendar.MINUTE, 0);
                    continue;
                }

                int dow = now.get(java.util.Calendar.DAY_OF_WEEK); // 1=SUNDAY
                int w = dow == 1 ? 7 : dow - 1; // 转换为 1=MON..7=SUN
                int dy = now.get(java.util.Calendar.DAY_OF_MONTH);
                int maxDay = now.getActualMaximum(java.util.Calendar.DAY_OF_MONTH);

                boolean dOk = !isDay || contains(days, dy);
                boolean wOk = !isWeek || contains(weeks, w);
                if (!dOk || !wOk || dy > maxDay) {
                    now.add(java.util.Calendar.DAY_OF_MONTH, 1);
                    now.set(java.util.Calendar.HOUR_OF_DAY, 0);
                    now.set(java.util.Calendar.MINUTE, 0);
                    continue;
                }

                int hr = now.get(java.util.Calendar.HOUR_OF_DAY);
                if (!contains(hrs, hr)) {
                    int nx = nextInArray(hrs, hr);
                    if (nx >= 0) now.set(java.util.Calendar.HOUR_OF_DAY, nx);
                    else { now.add(java.util.Calendar.DAY_OF_MONTH, 1); now.set(java.util.Calendar.HOUR_OF_DAY, hrs[0]); }
                    now.set(java.util.Calendar.MINUTE, 0);
                    continue;
                }

                int min = now.get(java.util.Calendar.MINUTE);
                if (!contains(mins, min)) {
                    int nx = nextInArray(mins, min);
                    if (nx >= 0) now.set(java.util.Calendar.MINUTE, nx);
                    else { now.add(java.util.Calendar.HOUR_OF_DAY, 1); now.set(java.util.Calendar.MINUTE, mins[0]); }
                    continue;
                }

                // 命中
                long delay = now.getTimeInMillis() - System.currentTimeMillis();
                return Math.max(delay, 100); // 至少延迟100ms
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static boolean contains(int[] arr, int val) {
        for (int v : arr) if (v == val) return true;
        return false;
    }

    private static int nextInArray(int[] arr, int val) {
        int best = -1;
        for (int v : arr) if (v > val && (best < 0 || v < best)) best = v;
        return best;
    }

    /**
     * 将 cron 字段展开为匹配值数组
     */
    private static int[] expand(String field, int max) {
        if ("*".equals(field) || "?".equals(field)) {
            int[] r = new int[max + 1];
            for (int i = 0; i <= max; i++) r[i] = i;
            return r;
        }
        if (field.startsWith("*/")) {
            int step = Integer.parseInt(field.substring(2));
            if (step <= 0) return new int[0];
            int count = max / step + 1;
            int[] r = new int[count];
            for (int i = 0, idx = 0; i <= max; i += step) r[idx++] = i;
            return r;
        }
        if (field.contains(",")) {
            String[] parts = field.split(",");
            int[] r = new int[parts.length];
            int idx = 0;
            for (String p : parts) r[idx++] = Integer.parseInt(p.trim());
            java.util.Arrays.sort(r);
            return r;
        }
        if (field.contains("-")) {
            String[] parts = field.split("-");
            int a = Integer.parseInt(parts[0]);
            int b = Integer.parseInt(parts[1]);
            if (a > b) return new int[0];
            int[] r = new int[b - a + 1];
            for (int i = a, idx = 0; i <= b; i++) r[idx++] = i;
            return r;
        }
        return new int[]{Integer.parseInt(field)};
    }

    // ========== 手动执行 / 停止 ==========

    /**
     * 停止正在运行的任务
     */
    public boolean stopTask(Long taskId) {
        Future<?> future = runningTasks.get(taskId);
        if (future != null && !future.isDone()) {
            boolean cancelled = future.cancel(true);
            runningTasks.remove(taskId);

            SyncTask task = taskRepository.findById(taskId).orElse(null);
            if (task != null) {
                task.setLastExecStatus("FAILED");
                task.setStatus("GENERATED");
                task.setLastExecTime(LocalDateTime.now());
                taskRepository.save(task);
            }

            StringBuffer logBuf = runningLogs.get(taskId);
            if (logBuf != null) {
                logBuf.append("\n[WARN] 任务已被手动停止\n");
                List<TaskExecution> execs = executionRepository.findByTaskIdOrderByStartedAtDesc(taskId);
                if (!execs.isEmpty()) {
                    TaskExecution exec = execs.get(0);
                    if ("RUNNING".equals(exec.getStatus()) || "PENDING".equals(exec.getStatus())) {
                        exec.setStatus("FAILED");
                        exec.setFinishedAt(LocalDateTime.now());
                        exec.setLogText(logBuf.toString());
                        executionRepository.save(exec);
                    }
                }
            }
            runningLogs.remove(taskId);
            return cancelled;
        }
        return false;
    }

    /**
     * 获取任务执行历史
     */
    public List<TaskExecution> getExecutionHistory(Long taskId, int limit) {
        List<TaskExecution> all = executionRepository.findByTaskIdOrderByStartedAtDesc(taskId);
        return all.stream().limit(limit).collect(Collectors.toList());
    }

    /**
     * 获取单次执行详情
     */
    public TaskExecution getExecution(Long execId) {
        return executionRepository.findById(execId).orElse(null);
    }

    /**
     * 任务结束后计算最终指标
     */
    private void calcExecMetricsFinal(SyncTask task, TaskExecution execution, String logText) {
        try {
            java.time.LocalDateTime startedAt = execution.getStartedAt();
            java.time.LocalDateTime finishedAt = execution.getFinishedAt();
            if (startedAt != null && finishedAt != null) {
                long durationSec = java.time.Duration.between(startedAt, finishedAt).getSeconds();
                task.setLastExecDuration(formatDuration(durationSec));
                // 优先取 Committed，没提交成功则回退到 Read Count
                long committed = parseLastMetric(logText, "Write Committed Count So Far");
                if (committed == 0) committed = parseLastMetric(logText, "Write Count So Far");
                long read = parseLastMetric(logText, "Read Count So Far");
                long attempt = parseLastMetric(logText, "Write Attempt Count So Far");
                long rows = committed > 0 ? committed : (read > 0 ? read : attempt);
                if (rows > 0) {
                    task.setLastExecRows(rows);
                    if (durationSec > 0) {
                        task.setLastExecQps(String.format("%.1f/s", (double) rows / durationSec));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 格式化秒数为可读字符串
     */
    private String formatDuration(long seconds) {
        if (seconds < 0) return "0秒";
        if (seconds < 60) return seconds + "秒";
        if (seconds < 3600) return (seconds / 60) + "分" + (seconds % 60) + "秒";
        return (seconds / 3600) + "时" + ((seconds % 3600) / 60) + "分" + (seconds % 60) + "秒";
    }

    /**
     * 从日志中解析指标的最后一次值
     */
    private long parseLastMetric(String logText, String metricName) {
        if (logText == null || logText.isEmpty()) return 0;
        String[] words = metricName.split("\\s+");
        StringBuilder regex = new StringBuilder();
        for (String w : words) {
            if (regex.length() > 0) regex.append("\\s+");
            regex.append(Pattern.quote(w));
        }
        regex.append("\\s*:\\s*(\\d+)"); // 必须有冒号
        Pattern p = Pattern.compile(regex.toString());
        Matcher m = p.matcher(logText);
        long last = 0;
        while (m.find()) {
            try { last = Long.parseLong(m.group(1)); } catch (NumberFormatException ignored) {}
        }
        return last;
    }

    /**
     * 解密配置文本中所有 AES 加密的密码
     */
    private String decryptConfigPasswords(String config) {
        if (config == null) return null;
        // 匹配 password = "AES:..." 或 password = "AES:..." 格式
        StringBuffer sb = new StringBuffer();
        Pattern p = Pattern.compile("password\\s*=\\s*\"(AES:[^\"]+)\"");
        Matcher m = p.matcher(config);
        while (m.find()) {
            String encrypted = m.group(1);
            String decrypted = AesUtil.decrypt(encrypted, secretKey);
            m.appendReplacement(sb, "password = \"" + Matcher.quoteReplacement(decrypted) + "\"");
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * 判断任务是否正在运行
     */
    public boolean isTaskRunning(Long taskId) {
        return runningTasks.containsKey(taskId) && !runningTasks.get(taskId).isDone();
    }

    /**
     * 任务执行完成后发送告警通知
     */
    private void sendAlertIfNeeded(SyncTask task, String status, String message) {
        if (alertService == null) return;
        try {
            alertService.sendTaskAlert(
                    task.getId(),
                    task.getTaskName(),
                    status,
                    message,
                    task.getLastExecDuration(),
                    task.getLastExecRows(),
                    task.getLastExecQps()
            );
        } catch (Exception e) {
            System.err.println("[SchedulerService] 告警发送失败: " + e.getMessage());
        }
    }
}
