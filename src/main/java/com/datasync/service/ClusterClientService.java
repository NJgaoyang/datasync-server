package com.datasync.service;

import com.datasync.entity.ClusterClient;
import com.datasync.repository.ClusterClientRepository;
import com.datasync.util.AesUtil;
import com.jcraft.jsch.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.util.*;

@Service
public class ClusterClientService {
    @Autowired
    private ClusterClientRepository repository;

    @Value("${datasync.secret-key:DataSync@2026!Key}")
    private String secretKey;

    public List<ClusterClient> list() {
        List<ClusterClient> list = repository.findAll();
        for (ClusterClient c : list) {
            c.setSshPassword(null);
        }
        return list;
    }

    @Transactional
    public ClusterClient save(ClusterClient client) {
        // 编辑时如果没填密码，保留旧密码
        if (client.getId() != null && (client.getSshPassword() == null || client.getSshPassword().isEmpty())) {
            ClusterClient old = repository.findById(client.getId()).orElse(null);
            if (old != null && old.getSshPassword() != null && old.getSshPassword().startsWith("AES:")) {
                client.setSshPassword(old.getSshPassword());
            }
        }
        // 新填的明文密码需要加密
        if (client.getSshPassword() != null && !client.getSshPassword().isEmpty()
                && !client.getSshPassword().startsWith("AES:")) {
            client.setSshPassword(AesUtil.encrypt(client.getSshPassword(), secretKey));
        }
        ClusterClient saved = repository.save(client);
        // 不能直接修改受管实体！否则事务提交时会把数据库密码清空
        ClusterClient result = new ClusterClient();
        result.setId(saved.getId());
        result.setClientName(saved.getClientName());
        result.setHost(saved.getHost());
        result.setPort(saved.getPort());
        result.setStatus(saved.getStatus());
        result.setCpuUsage(saved.getCpuUsage());
        result.setMemoryUsage(saved.getMemoryUsage());
        result.setDescription(saved.getDescription());
        result.setSshPort(saved.getSshPort());
        result.setSshUser(saved.getSshUser());
        result.setSeatunnelHome(saved.getSeatunnelHome());
        result.setCreatedAt(saved.getCreatedAt());
        result.setUpdatedAt(saved.getUpdatedAt());
        // 不复制 sshPassword，前端永远看不到密码
        return result;
    }

    @Transactional
    public void delete(Long id) {
        repository.deleteById(id);
    }

    public ClusterClient getById(Long id) {
        ClusterClient c = repository.findById(id).orElse(null);
        if (c != null) c.setSshPassword(null);
        return c;
    }

    /**
     * 真实健康检查：SSH 连接到远程服务器，检测 SeaTunnel 是否可用
     */
    @Transactional
    public Map<String, Object> checkHealth(Long id) {
        ClusterClient client = repository.findById(id).orElseThrow(() -> new RuntimeException("客户端不存在"));
        Map<String, Object> result = new java.util.HashMap<>();

        if (client.getSshUser() == null || client.getSshUser().isEmpty()) {
            client.setStatus("OFFLINE"); client.setCpuUsage(0.0); client.setMemoryUsage(0.0);
            repository.save(client);
            result.put("status", "OFFLINE");
            result.put("cpuUsage", 0.0);
            result.put("memoryUsage", 0.0);
            result.put("message", "未配置SSH用户");
            return result;
        }

        JSch jsch = new JSch();
        Session session = null;
        try {
            int sshPort = client.getSshPort() != null ? client.getSshPort() : 22;
            session = jsch.getSession(client.getSshUser(), client.getHost(), sshPort);

            String sshPwd = client.getSshPassword();
            if (sshPwd != null && sshPwd.startsWith("AES:")) {
                sshPwd = AesUtil.decrypt(sshPwd, secretKey);
            }
            final String pwd = sshPwd;
            if (pwd == null || pwd.isEmpty()) {
                client.setStatus("OFFLINE");
                result.put("message", "SSH密码为空，请编辑客户端重新填写密码");
                return result;
            }
            System.out.println("[HealthCheck] host=" + client.getHost() + " user=" + client.getSshUser() + " port=" + (client.getSshPort() != null ? client.getSshPort() : 22) + " pwdLen=" + (pwd != null ? pwd.length() : 0));

            session.setPassword(pwd);
            session.setUserInfo(new SshUserInfo(pwd));

            // 兼容现代SSH服务器的算法
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "keyboard-interactive,password");
            session.setConfig("kex", "diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group-exchange-sha256,ecdh-sha2-nistp256,ecdh-sha2-nistp384,ecdh-sha2-nistp521,diffie-hellman-group14-sha256");
            session.setConfig("server_host_key", "ssh-rsa,ssh-dss,ecdsa-sha2-nistp256,ecdsa-sha2-nistp384,ecdsa-sha2-nistp521");
            session.setConfig("cipher.s2c", "aes128-ctr,aes192-ctr,aes256-ctr,aes128-gcm@openssh.com,aes256-gcm@openssh.com");
            session.setConfig("cipher.c2s", "aes128-ctr,aes192-ctr,aes256-ctr,aes128-gcm@openssh.com,aes256-gcm@openssh.com");
            session.setConfig("mac.s2c", "hmac-sha2-256,hmac-sha2-512,hmac-sha1");
            session.setConfig("mac.c2s", "hmac-sha2-256,hmac-sha2-512,hmac-sha1");

            session.connect(10000);

            // 检查 SeaTunnel 脚本是否存在
            String stHome = client.getSeatunnelHome();
            if (stHome == null || stHome.isEmpty()) {
                stHome = "/data/software/apache-seatunnel-2.3.13";
            }

            ChannelExec exec = (ChannelExec) session.openChannel("exec");
            exec.setCommand("test -f " + stHome + "/bin/seatunnel.sh && echo ST_OK || echo ST_MISSING");
            exec.connect(5000);
            BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream()));
            String checkResult = reader.readLine();
            exec.disconnect();

            if ("ST_OK".equals(checkResult)) {
                client.setStatus("ONLINE");
                result.put("message", "SSH连接成功, SeaTunnel就绪");
            } else {
                client.setStatus("OFFLINE");
                result.put("message", "SSH连接成功, 但未找到 seatunnel.sh: " + stHome + "/bin/seatunnel.sh");
            }

            // CPU/内存
            try {
                ChannelExec resExec = (ChannelExec) session.openChannel("exec");
                resExec.setCommand("top -bn1 | grep 'Cpu(s)' | awk '{print $2+$4}' && free -m | awk 'NR==2{printf \"%.1f\", $3*100/$2}'");
                resExec.connect(5000);
                BufferedReader resReader = new BufferedReader(new InputStreamReader(resExec.getInputStream()));
                String cpuStr = resReader.readLine(); String memStr = resReader.readLine();
                resExec.disconnect();
                if (cpuStr != null) try { client.setCpuUsage(Double.parseDouble(cpuStr.trim())); } catch (NumberFormatException ignored) {}
                if (memStr != null) try { client.setMemoryUsage(Double.parseDouble(memStr.trim())); } catch (NumberFormatException ignored) {}
            } catch (Exception ignored) {}

        } catch (Exception e) {
            client.setStatus("OFFLINE");
            client.setCpuUsage(0.0);
            client.setMemoryUsage(0.0);
            result.put("message", "SSH连接失败: " + e.getMessage());
        } finally {
            if (session != null && session.isConnected()) session.disconnect();
        }

        repository.save(client);
        result.put("status", client.getStatus());
        result.put("cpuUsage", client.getCpuUsage() != null ? client.getCpuUsage() : 0.0);
        result.put("memoryUsage", client.getMemoryUsage() != null ? client.getMemoryUsage() : 0.0);
        return result;
    }

    /**
     * 批量健康检查所有配置了SSH的客户端
     */
    @Transactional
    public void checkAllHealth() {
        List<ClusterClient> all = repository.findAll();
        for (ClusterClient c : all) {
            if (c.getSshUser() != null && !c.getSshUser().isEmpty()) {
                try { checkHealth(c.getId()); } catch (Exception ignored) {}
            }
        }
    }
}
