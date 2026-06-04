package com.datasync.config;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LoginInterceptor implements HandlerInterceptor {

    /** token → username:role */
    public static final Map<String, String> SESSIONS = new ConcurrentHashMap<>();

    /**
     * 生成新 token 并保存会话
     */
    public static String createSession(String username, String role) {
        String token = UUID.randomUUID().toString().replace("-", "");
        SESSIONS.put(token, username + ":" + role);
        return token;
    }

    /** 获取用户名 */
    public static String getUsername(String token) {
        String val = SESSIONS.get(token);
        if (val == null) return null;
        int idx = val.lastIndexOf(':');
        return idx > 0 ? val.substring(0, idx) : val;
    }

    /** 获取角色 */
    public static String getRole(String token) {
        String val = SESSIONS.get(token);
        if (val == null) return null;
        int idx = val.lastIndexOf(':');
        return idx > 0 ? val.substring(idx + 1) : "USER";
    }

    /**
     * 移除会话
     */
    public static void removeSession(String token) {
        SESSIONS.remove(token);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String path = request.getRequestURI();

        // 登录接口和静态资源放行
        if (path.startsWith("/api/auth/")) return true;
        if (path.endsWith(".html") || path.endsWith(".js") || path.endsWith(".css")
                || path.endsWith(".png") || path.endsWith(".ico") || path.endsWith(".woff2")) {
            return true;
        }

        // API 请求校验
        String token = request.getHeader("X-Auth-Token");
        if (token == null || !SESSIONS.containsKey(token)) {
            response.setStatus(401);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"success\":false,\"message\":\"未登录或会话已过期\"}");
            return false;
        }

        // READONLY 角色只能执行 GET 请求
        String role = getRole(token);
        if ("READONLY".equals(role) && !"GET".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(403);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"success\":false,\"message\":\"只读用户不允许执行此操作\"}");
            return false;
        }
        return true;
    }
}
