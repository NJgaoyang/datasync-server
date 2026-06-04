package com.datasync.service;

import com.datasync.entity.OperationLog;
import com.datasync.repository.OperationLogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class OperationLogService {
    @Autowired private OperationLogRepository logRepository;

    public void save(OperationLog log) {
        logRepository.save(log);
    }

    public Map<String, Object> search(String keyword, String startDate, String endDate, int page, int size, String username) {
        Pageable pageable = PageRequest.of(page, size);
        String kw = (keyword == null || keyword.trim().isEmpty()) ? null : keyword.trim();
        String sd = (startDate == null || startDate.trim().isEmpty()) ? null : startDate.trim();
        String ed = (endDate == null || endDate.trim().isEmpty()) ? null : endDate.trim();
        String un = (username == null || username.trim().isEmpty()) ? null : username.trim();
        Page<OperationLog> result = logRepository.search(kw, sd, ed, un, pageable);
        Map<String, Object> map = new HashMap<>();
        map.put("content", result.getContent());
        map.put("totalElements", result.getTotalElements());
        map.put("totalPages", result.getTotalPages());
        map.put("number", result.getNumber());
        return map;
    }
}
