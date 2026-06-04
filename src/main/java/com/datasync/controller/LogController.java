package com.datasync.controller;

import com.datasync.service.OperationLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/log")
public class LogController {
    @Autowired private OperationLogService logService;

    @GetMapping("/page")
    public Map<String, Object> page(@RequestParam(defaultValue = "") String keyword,
                                     @RequestParam(defaultValue = "0") int page,
                                     @RequestParam(defaultValue = "15") int size,
                                     @RequestParam(required = false) String startDate,
                                     @RequestParam(required = false) String endDate,
                                     @RequestParam(required = false) String username) {
        return logService.search(keyword, startDate, endDate, page, size, username);
    }
}
