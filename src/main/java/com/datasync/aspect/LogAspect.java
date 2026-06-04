package com.datasync.aspect;

import com.datasync.entity.OperationLog;
import com.datasync.entity.SysUser;
import com.datasync.repository.SysUserRepository;
import com.datasync.service.OperationLogService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

@Aspect
@Component
public class LogAspect {
    @Autowired private OperationLogService logService;
    @Autowired private SysUserRepository userRepository;
    @Autowired(required = false) private HttpServletRequest request;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Around("@annotation(org.springframework.web.bind.annotation.PostMapping) || " +
            "@annotation(org.springframework.web.bind.annotation.PutMapping) || " +
            "@annotation(org.springframework.web.bind.annotation.DeleteMapping)")
    public Object logOperation(ProceedingJoinPoint jp) throws Throwable {
        String path = request != null ? request.getRequestURI() : "";
        if (path.contains("/log/") || path.contains("/stats/") || path.contains("/executions/")
                || (path.contains("/user/") && (path.contains("list") || path.contains("permissions")))) {
            return jp.proceed();
        }

        OperationLog log = new OperationLog();
        log.setUsername("admin");
        log.setIp(request != null ? request.getRemoteAddr() : "127.0.0.1");
        log.setCreatedAt(LocalDateTime.now());

        // 提取请求体中的关键信息
        Object[] args = jp.getArgs();
        String bodyName = extractName(args);
        String taskId = extractPathId(path);

        // 根据路径判断
        if (path.contains("/task/")) {
            String taskRef = "任务" + (taskId != null ? "#" + taskId : "");
            log.setTarget(taskRef);
            if (path.contains("/execute")) { log.setAction("执行任务"); log.setDetail("手动触发执行" + taskRef); }
            else if (path.contains("/stop")) { log.setAction("停止任务"); log.setDetail("手动停止" + taskRef); }
            else if (path.contains("/toggle")) { log.setAction("上下线切换"); log.setDetail("切换" + taskRef + "上下线状态"); }
            else if (path.contains("/create")) { log.setAction("创建任务"); log.setDetail("新增同步任务" + (bodyName != null ? "：" + bodyName : "")); }
            else if (path.endsWith("/task/") && jp.getSignature().getName().contains("delete")) { log.setAction("删除任务"); log.setDetail("删除" + taskRef); }
            else { log.setAction("任务操作"); log.setDetail("操作" + taskRef); }
        } else if (path.contains("/datasource/")) {
            String dsRef = bodyName != null ? bodyName : "";
            log.setTarget(dsRef);
            if (path.contains("/save")) { log.setAction("新增数据源"); log.setDetail("新增数据源：" + dsRef); }
            else if (path.contains("/update")) { log.setAction("编辑数据源"); log.setDetail("编辑数据源：" + dsRef); }
            else if (path.contains("/delete") || path.matches(".*/datasource/\\d+$")) { log.setAction("删除数据源"); log.setDetail("删除数据源"); }
            else { log.setAction("数据源操作"); log.setDetail("数据源操作：" + dsRef); }
        } else if (path.contains("/user/")) {
            log.setTarget(bodyName != null ? bodyName : (taskId != null ? "用户#" + taskId : "系统"));
            if (path.contains("/save")) { log.setAction("新增用户"); log.setDetail("新增用户：" + bodyName); }
            else if (path.contains("/update")) { log.setAction("编辑用户"); log.setDetail("编辑用户：" + bodyName); }
            else if (path.contains("/delete")) { log.setAction("删除用户"); log.setDetail("删除用户"); }
            else if (path.contains("/grant") || path.contains("/revoke")) {
                String granteeName = extractGranteeName(args);
                boolean isGrant = path.contains("/grant");
                log.setTarget(granteeName != null ? granteeName : "未知用户");
                log.setAction(isGrant ? "授权" : "撤销授权");
                log.setDetail((isGrant ? "为" : "撤销") + (granteeName != null ? granteeName : "用户") + (isGrant ? "授权数据源访问" : "的数据源权限"));
            }
            else { log.setAction("用户操作"); log.setDetail("用户相关操作"); }
        } else if (path.contains("/metadata/")) {
            log.setTarget(bodyName != null ? bodyName : "元数据");
            log.setAction("元数据操作");
            log.setDetail("执行元数据相关操作");
        } else {
            log.setTarget(bodyName != null ? bodyName : path);
            log.setAction("系统操作");
            log.setDetail(path);
        }

        Object result;
        try {
            result = jp.proceed();
            log.setSuccess(true);
        } catch (Exception e) {
            log.setSuccess(false);
            log.setDetail((log.getDetail() != null ? log.getDetail() : "") + " - 失败：" + e.getMessage());
            throw e;
        }

        logService.save(log);
        return result;
    }

    /** 从请求参数中提取 name/username/taskName */
    private String extractName(Object[] args) {
        if (args == null) return null;
        for (Object arg : args) {
            if (arg == null) continue;
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = mapper.convertValue(arg, Map.class);
                if (map.containsKey("name")) return String.valueOf(map.get("name"));
                if (map.containsKey("taskName")) return String.valueOf(map.get("taskName"));
                if (map.containsKey("username")) return String.valueOf(map.get("username"));
            } catch (Exception ignored) {}
        }
        return null;
    }

    /** 从路径提取数字 ID */
    private String extractPathId(String path) {
        if (path == null) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("/(\\d+)").matcher(path);
        return m.find() ? m.group(1) : null;
    }

    /** 从请求体提取 userId 并查用户名 */
    private String extractGranteeName(Object[] args) {
        if (args == null) return null;
        for (Object arg : args) {
            if (arg == null) continue;
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = mapper.convertValue(arg, Map.class);
                if (map.containsKey("userId")) {
                    Long uid = Long.valueOf(map.get("userId").toString());
                    Optional<SysUser> u = userRepository.findById(uid);
                    return u.map(SysUser::getUsername).orElse("用户#" + uid);
                }
            } catch (Exception ignored) {}
        }
        return null;
    }
}
