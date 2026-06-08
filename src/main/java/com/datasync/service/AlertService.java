package com.datasync.service;

import com.datasync.entity.AlertConfig;
import com.datasync.repository.AlertConfigRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.annotation.PostConstruct;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 告警推送服务
 * 支持钉钉(DingTalk)和飞书(Feishu) Webhook 推送，支持自定义模板和加签
 */
@Service
public class AlertService {

    @Autowired
    private AlertConfigRepository alertConfigRepository;

    @Autowired
    private RestTemplate restTemplate;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ==================== 默认模板 ====================

    private static final String DEFAULT_DINGTALK_FAILURE_TEMPLATE =
            "### ⚠️ DataSync 任务告警\n\n" +
            "- **任务名称：** {taskName}\n" +
            "- **任务状态：** <font color=\"#FF0000\">❌ 失败</font>\n" +
            "- **执行耗时：** {duration}\n" +
            "- **失败原因：** {message}\n" +
            "- **告警时间：** {time}\n";

    private static final String DEFAULT_DINGTALK_SUCCESS_TEMPLATE =
            "### ✅ DataSync 任务通知\n\n" +
            "- **任务名称：** {taskName}\n" +
            "- **任务状态：** ✅ 成功\n" +
            "- **执行耗时：** {duration}\n" +
            "- **通知时间：** {time}\n";

    private static final String DEFAULT_FEISHU_FAILURE_TEMPLATE =
            "⚠️ DataSync 任务告警\n" +
            "任务名称：{taskName}\n" +
            "任务状态：<font color='red'>失败</font>\n" +
            "执行耗时：{duration}\n" +
            "失败原因：{message}\n" +
            "告警时间：{time}";

    private static final String DEFAULT_FEISHU_SUCCESS_TEMPLATE =
            "✅ DataSync 任务通知\n" +
            "任务名称：{taskName}\n" +
            "任务状态：成功\n" +
            "执行耗时：{duration}\n" +
            "通知时间：{time}";

    // ==================== 公开 API ====================

    /**
     * 发送任务告警（根据 enabled 配置 + eventTypes 匹配自动过滤）
     */
    public void sendTaskAlert(Long taskId, String taskName, String status, String message,
                              String duration, Long rows, String qps) {
        List<AlertConfig> configs = alertConfigRepository.findByEnabledTrue();
        for (AlertConfig cfg : configs) {
            if (!shouldAlert(cfg.getEventTypes(), status)) {
                continue;
            }
            try {
                Map<String, String> vars = buildVars(taskId, taskName, status, message, duration, rows, qps);
                String content = renderTemplate(cfg, status, vars);

                if ("DINGTALK".equalsIgnoreCase(cfg.getType())) {
                    sendDingTalk(cfg, content, status);
                } else if ("FEISHU".equalsIgnoreCase(cfg.getType())) {
                    sendFeishu(cfg, content, status);
                }
            } catch (Exception e) {
                System.err.println("[AlertService] 发送告警失败 [" + cfg.getName() + "]: " + e.getMessage());
            }
        }
    }

    /**
     * 测试告警配置是否可用，失败时抛出异常并返回详细错误信息
     */
    public Map<String, Object> testAlert(AlertConfig cfg) {
        Map<String, String> vars = new HashMap<>();
        vars.put("taskName", "测试任务");
        vars.put("status", "SUCCESS");
        vars.put("message", "这是一条测试消息");
        vars.put("duration", "1秒");
        vars.put("rows", "100");
        vars.put("qps", "100.0/s");
        vars.put("time", LocalDateTime.now().format(DT_FMT));

        String content;
        if (cfg.getTemplate() != null && !cfg.getTemplate().trim().isEmpty()) {
            content = renderVars(cfg.getTemplate(), vars);
        } else {
            if ("FEISHU".equalsIgnoreCase(cfg.getType())) {
                content = renderVars(DEFAULT_FEISHU_SUCCESS_TEMPLATE, vars);
            } else {
                content = renderVars(DEFAULT_DINGTALK_SUCCESS_TEMPLATE, vars);
            }
        }

        String rawResp;
        if ("DINGTALK".equalsIgnoreCase(cfg.getType())) {
            rawResp = sendDingTalkWithResult(cfg, content, "SUCCESS");
        } else if ("FEISHU".equalsIgnoreCase(cfg.getType())) {
            rawResp = sendFeishuWithResult(cfg, content, "SUCCESS");
        } else {
            throw new RuntimeException("不支持的告警类型: " + cfg.getType());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("message", "测试消息发送成功");
        result.put("rawResponse", rawResp);
        return result;
    }

    // ==================== 配置 CRUD 代理 ====================

    public List<AlertConfig> list() {
        return alertConfigRepository.findAll();
    }

    public AlertConfig save(AlertConfig cfg) {
        return alertConfigRepository.save(cfg);
    }

    public AlertConfig update(AlertConfig cfg) {
        AlertConfig existing = alertConfigRepository.findById(cfg.getId())
                .orElseThrow(() -> new RuntimeException("告警配置不存在"));
        existing.setName(cfg.getName());
        existing.setType(cfg.getType().toUpperCase());
        existing.setWebhookUrl(cfg.getWebhookUrl());
        existing.setSecret(cfg.getSecret());
        existing.setKeyword(cfg.getKeyword());
        existing.setTemplate(cfg.getTemplate());
        existing.setEnabled(cfg.getEnabled());
        existing.setEventTypes(cfg.getEventTypes());
        return alertConfigRepository.save(existing);
    }

    public void delete(Long id) {
        alertConfigRepository.deleteById(id);
    }

    public AlertConfig toggle(Long id) {
        AlertConfig cfg = alertConfigRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("告警配置不存在"));
        cfg.setEnabled(!Boolean.TRUE.equals(cfg.getEnabled()));
        return alertConfigRepository.save(cfg);
    }

    // ==================== 内部实现 ====================

    private boolean shouldAlert(String eventTypes, String status) {
        if (eventTypes == null || "ALL".equalsIgnoreCase(eventTypes)) return true;
        // 任务状态 FAILED 对应事件类型 FAILURE，做一次归一化
        String normalized = "FAILED".equalsIgnoreCase(status) ? "FAILURE" : status.toUpperCase();
        String[] types = eventTypes.toUpperCase().split(",");
        for (String t : types) {
            if (t.trim().equalsIgnoreCase(normalized)) return true;
        }
        return false;
    }

    private Map<String, String> buildVars(Long taskId, String taskName, String status,
                                          String message, String duration, Long rows, String qps) {
        Map<String, String> vars = new LinkedHashMap<>();
        vars.put("taskId", taskId != null ? taskId.toString() : "-");
        vars.put("taskName", taskName != null ? taskName : "-");
        vars.put("status", status != null ? status : "-");
        vars.put("message", message != null ? message : "-");
        vars.put("duration", duration != null ? duration : "-");
        vars.put("rows", rows != null ? rows.toString() : "-");
        vars.put("qps", qps != null ? qps : "-");
        vars.put("time", LocalDateTime.now().format(DT_FMT));
        return vars;
    }

    private String renderTemplate(AlertConfig cfg, String status, Map<String, String> vars) {
        if (cfg.getTemplate() != null && !cfg.getTemplate().trim().isEmpty()) {
            return renderVars(cfg.getTemplate(), vars);
        }
        // 使用默认模板
        if ("DINGTALK".equalsIgnoreCase(cfg.getType())) {
            if ("FAILED".equalsIgnoreCase(status)) {
                return renderVars(DEFAULT_DINGTALK_FAILURE_TEMPLATE, vars);
            }
            return renderVars(DEFAULT_DINGTALK_SUCCESS_TEMPLATE, vars);
        } else {
            if ("FAILED".equalsIgnoreCase(status)) {
                return renderVars(DEFAULT_FEISHU_FAILURE_TEMPLATE, vars);
            }
            return renderVars(DEFAULT_FEISHU_SUCCESS_TEMPLATE, vars);
        }
    }

    private String renderVars(String template, Map<String, String> vars) {
        String result = template;
        for (Map.Entry<String, String> e : vars.entrySet()) {
            result = result.replace("{" + e.getKey() + "}", e.getValue() != null ? e.getValue() : "");
        }
        return result;
    }

    // ---------- 钉钉 Webhook ----------

    @SuppressWarnings("unchecked")
    private void sendDingTalk(AlertConfig cfg, String content, String status) {
        try {
            sendDingTalkWithResult(cfg, content, status);
        } catch (Exception e) {
            System.err.println("[AlertService] 钉钉推送异常: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private String sendDingTalkWithResult(AlertConfig cfg, String content, String status) {
        try {
            String url = cfg.getWebhookUrl();

            // 加签
            if (cfg.getSecret() != null && !cfg.getSecret().trim().isEmpty()) {
                long timestamp = System.currentTimeMillis();
                String sign = dingTalkSign(timestamp, cfg.getSecret());
                url += (url.contains("?") ? "&" : "?") + "timestamp=" + timestamp + "&sign=" + sign;
            }

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("msgtype", "markdown");

            // 钉钉关键词校验：如果设置了关键词，拼到消息最前面
            String text = content;
            if (cfg.getKeyword() != null && !cfg.getKeyword().trim().isEmpty()) {
                text = cfg.getKeyword().trim() + "\n" + content;
            }

            Map<String, String> markdown = new LinkedHashMap<>();
            markdown.put("title", "DataSync 告警通知");
            markdown.put("text", text);
            body.put("markdown", markdown);

            Map<String, Object> at = new LinkedHashMap<>();
            at.put("isAtAll", false);
            body.put("at", at);

            String resp = restTemplate.postForObject(url, body, String.class);
            Map<String, Object> respMap = MAPPER.readValue(resp, Map.class);
            Object errcode = respMap.get("errcode");
            if (errcode != null && ((Number) errcode).intValue() != 0) {
                String errMsg = respMap.get("errmsg") != null ? respMap.get("errmsg").toString() : "未知错误";
                throw new RuntimeException("钉钉返回错误 [errcode=" + errcode + "]: " + errMsg);
            }
            return resp;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("钉钉推送网络异常: " + e.getMessage(), e);
        }
    }

    private String dingTalkSign(long timestamp, String secret) throws Exception {
        String stringToSign = timestamp + "\n" + secret;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(stringToSign.getBytes("UTF-8"));
        String sign = Base64.getEncoder().encodeToString(signData);
        return URLEncoder.encode(sign, "UTF-8");
    }

    // ---------- 飞书 Webhook ----------

    @SuppressWarnings("unchecked")
    private void sendFeishu(AlertConfig cfg, String content, String status) {
        try {
            sendFeishuWithResult(cfg, content, status);
        } catch (Exception e) {
            System.err.println("[AlertService] 飞书推送异常: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private String sendFeishuWithResult(AlertConfig cfg, String content, String status) {
        try {
            String url = cfg.getWebhookUrl();

            // 飞书新版卡片消息格式（富文本）
            List<Map<String, Object>> elements = new ArrayList<>();

            // 标题
            boolean isFailure = "FAILED".equalsIgnoreCase(status);
            String title = isFailure ? "⚠️ DataSync 任务告警" : "✅ DataSync 任务通知";
            String titleColor = isFailure ? "red" : "green";

            Map<String, Object> header = new LinkedHashMap<>();
            header.put("template", titleColor);
            Map<String, Object> titleObj = new LinkedHashMap<>();
            titleObj.put("tag", "plain_text");
            titleObj.put("content", title);
            header.put("title", titleObj);

            Map<String, Object> card = new LinkedHashMap<>();
            card.put("header", header);

            // 内容区域
            Map<String, Object> div = new LinkedHashMap<>();
            div.put("tag", "div");
            Map<String, Object> textField = new LinkedHashMap<>();
            textField.put("tag", "lark_md");
            textField.put("content", content);
            div.put("text", textField);
            elements.add(div);

            // 分割线 + 时间戳
            Map<String, Object> hr = new LinkedHashMap<>();
            hr.put("tag", "hr");
            elements.add(hr);

            Map<String, Object> note = new LinkedHashMap<>();
            note.put("tag", "note");
            Map<String, Object> noteText = new LinkedHashMap<>();
            noteText.put("tag", "plain_text");
            noteText.put("content", "DataSync Platform · " + LocalDateTime.now().format(DT_FMT));
            note.put("elements", Collections.singletonList(noteText));
            elements.add(note);

            card.put("elements", elements);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("msg_type", "interactive");
            body.put("card", card);

            // 飞书加签
            if (cfg.getSecret() != null && !cfg.getSecret().trim().isEmpty()) {
                long timestamp = System.currentTimeMillis() / 1000;
                String sign = feishuSign(timestamp, cfg.getSecret());
                Map<String, String> signHeader = new LinkedHashMap<>();
                signHeader.put("timestamp", String.valueOf(timestamp));
                signHeader.put("sign", sign);
                body.put("sign", signHeader);
            }

            String resp = restTemplate.postForObject(url, body, String.class);
            Map<String, Object> respMap = MAPPER.readValue(resp, Map.class);
            Object code = respMap.get("code");
            if (code != null && ((Number) code).intValue() != 0) {
                String msg = respMap.get("msg") != null ? respMap.get("msg").toString() : "未知错误";
                throw new RuntimeException("飞书返回错误 [code=" + code + "]: " + msg);
            }
            return resp;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("飞书推送网络异常: " + e.getMessage(), e);
        }
    }

    private String feishuSign(long timestamp, String secret) throws Exception {
        String stringToSign = timestamp + "\n" + secret;
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(stringToSign.getBytes("UTF-8"), "HmacSHA256"));
        byte[] signData = mac.doFinal(new byte[0]);
        // 飞书签名: HMAC-SHA256("" , timestamp+"\n"+secret) -> Base64
        mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA256"));
        signData = mac.doFinal(String.valueOf(timestamp).getBytes("UTF-8"));
        return Base64.getEncoder().encodeToString(signData);
    }
}
