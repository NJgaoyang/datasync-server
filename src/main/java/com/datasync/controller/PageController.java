package com.datasync.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 前端 SPA 路由 fallback，所有非 /api/ 路径转发到 index.html
 */
@Controller
public class PageController {

    @GetMapping(value = {"/", "/login", "/dashboard", "/batch", "/realtime", "/log",
            "/datasource", "/metadata", "/create", "/users", "/perm", "/oplog"})
    public String forwardToIndex() {
        return "forward:/index.html";
    }
}
