# DataSync Server — 告警模块升级文档

## 版本信息

| 项目 | 说明 |
|------|------|
| 项目名称 | datasync-server |
| 技术栈 | Spring Boot 2.7.18 + JPA + MySQL |
| Java 版本 | 1.8 |
| 升级内容 | 新增钉钉/飞书 Webhook 告警模块 |
| 升级日期 | 2026-06-08 |

---

## 一、变更文件清单

### 新增文件（4 个）

```
src/main/java/com/datasync/entity/AlertConfig.java          — 告警配置 JPA 实体（表名 alert_config）
src/main/java/com/datasync/repository/AlertConfigRepository.java — JPA Repository
src/main/java/com/datasync/service/AlertService.java        — 告警推送核心服务（钉钉/飞书 Webhook + 加签）
src/main/java/com/datasync/controller/AlertController.java  — 告警配置 REST API 控制器
```

### 修改文件（3 个）

```
src/main/java/com/datasync/service/SchedulerService.java    — 任务执行成功/失败后自动触发告警
src/main/resources/static/index.html                        — 前端侧边栏新增「告警配置」页面
src/main/resources/application.yml                          — 补充告警模块 API 文档注释
```

---

## 二、数据库变更

无需手动执行 SQL。`application.yml` 中 `jpa.hibernate.ddl-auto: update` 已开启，**服务启动时 JPA 自动创建 `alert_config` 表**。

`alert_config` 表结构：

```sql
CREATE TABLE IF NOT EXISTS alert_config (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(100)  NOT NULL    COMMENT '告警配置名称',
    type          VARCHAR(20)   NOT NULL    COMMENT '渠道类型: DINGTALK / FEISHU',
    webhook_url   VARCHAR(500)  NOT NULL    COMMENT 'Webhook 地址',
    secret        VARCHAR(200)              COMMENT '签名密钥（可选）',
    keyword       VARCHAR(100)              COMMENT '钉钉机器人关键词（可选）',
    template      TEXT                      COMMENT '自定义告警模板（可选）',
    enabled       BIT           NOT NULL    COMMENT '是否启用',
    event_types   VARCHAR(50)   NOT NULL    COMMENT '触发事件: FAILURE / SUCCESS / ALL',
    created_at    DATETIME                  COMMENT '创建时间',
    updated_at    DATETIME                  COMMENT '更新时间'
);
```

---

## 三、Git 提交步骤

```bash
# 1. 进入项目目录
cd /path/to/datasync-server

# 2. 查看变更（确认无意外文件）
git status

# 3. 添加告警模块的新增和修改文件
git add src/main/java/com/datasync/entity/AlertConfig.java
git add src/main/java/com/datasync/repository/AlertConfigRepository.java
git add src/main/java/com/datasync/service/AlertService.java
git add src/main/java/com/datasync/controller/AlertController.java
git add src/main/java/com/datasync/service/SchedulerService.java
git add src/main/resources/static/index.html
git add src/main/resources/application.yml
git add UPGRADE.md

# 4. 确认 .gitignore 已忽略不需要的文件（target/ 和 .idea/）
# 如果未配置，先添加：
# echo "target/" >> .gitignore
# echo ".idea/" >> .gitignore

# 5. 提交
git commit -m "feat: 新增钉钉/飞书 Webhook 告警模块

- 新增 AlertConfig 实体、Repository、Service、Controller
- SchedulerService 任务执行成功/失败后自动触发告警
- 前端新增「告警配置」页面（仅 ADMIN 可见）
- 支持自定义模板、加签校验、钉钉关键词校验
- 修复 FAILED/FAILURE 状态匹配 bug"

# 6. 推送到远程仓库
git push origin master
```

---

## 四、生产环境部署步骤

### 4.1 构建新版本

```bash
# 在项目根目录执行
mvn clean package -DskipTests

# 产物位置
ls -la target/datasync-server-1.0.0.jar
```

### 4.2 停服

```bash
# 方式一：systemd（推荐）
sudo systemctl stop datasync-server

# 方式二：直接杀进程
ps -ef | grep datasync-server | grep -v grep | awk '{print $2}' | xargs kill
```

### 4.3 备份旧版本（重要！）

```bash
# 备份 jar
cp /data/datasync-server/datasync-server.jar /data/backup/datasync-server.jar.$(date +%Y%m%d_%H%M%S)

# 备份数据库（推荐先备份 alert_config 会在启动时自动创建）
mysqldump -h rm-uf6r5izg7v0hw4maseo.mysql.rds.aliyuncs.com \
          -u yzl -p datasync > /data/backup/datasync_db_$(date +%Y%m%d_%H%M%S).sql
```

### 4.4 更新部署

```bash
# 上传新 jar
scp target/datasync-server-1.0.0.jar user@your-server:/data/datasync-server/datasync-server.jar

# 启动服务
sudo systemctl start datasync-server

# 查看启动日志（确认无异常）
sudo journalctl -u datasync-server -f
```

### 4.5 验证部署

1. **确认服务启动正常**
   ```bash
   curl http://localhost:9000/actuator/health
   # 或直接访问 http://your-server:9000
   ```

2. **确认 alert_config 表已自动创建**
   ```sql
   DESC alert_config;
   ```

3. **登录前端，验证告警配置页面**
   - 使用 ADMIN 账号登录
   - 左侧菜单「系统」→「告警配置」
   - 新增一条钉钉配置并点击「测试」

---

## 五、使用说明

### 5.1 新建告警配置

1. 管理员登录 → 告警配置 → 新增配置
2. 填写：
   - **配置名称**：如「生产环境钉钉告警」
   - **渠道类型**：钉钉 / 飞书
   - **触发事件**：仅失败 / 仅成功 / 全部
   - **Webhook 地址**：钉钉/飞书机器人的 Webhook URL
   - **签名密钥**（可选）：机器人加签密钥
   - **钉钉关键词**（可选）：若钉钉机器人设置了关键词校验需填写
   - **自定义模板**（可选）：支持占位符 `{taskName}` `{status}` `{message}` `{duration}` `{rows}` `{qps}` `{time}`，留空使用默认模板
3. 保存后点「测试」验证配置

### 5.2 告警触发时机

- 任务执行 **成功** / **失败** 后自动推送
- 由 `SchedulerService.executeTaskInternal()` 自动调用
- 手动执行和定时调度均覆盖

### 5.3 默认模板预览

**钉钉 - 失败**：
```
⚠️ DataSync 任务告警
- 任务名称：xxx
- 任务状态：失败（红色高亮）
- 执行耗时：xxx
- 失败原因：xxx
- 告警时间：2026-06-08 09:30:00
```

**钉钉 - 成功**：
```
✅ DataSync 任务通知
- 任务名称：xxx
- 任务状态：成功
- 执行耗时：xxx
- 通知时间：2026-06-08 09:30:00
```

---

## 六、回滚方案

如需回滚到旧版本：

```bash
# 1. 停服
sudo systemctl stop datasync-server

# 2. 恢复旧 jar
cp /data/backup/datasync-server.jar.20260608_xxxxxx /data/datasync-server/datasync-server.jar

# 3. 启动
sudo systemctl start datasync-server

# 4. （可选）保留或删除 alert_config 表，不影响旧版本运行
# 旧版本代码不会访问此表，可保留不删
```

---

## 七、注意事项

| 事项 | 说明 |
|------|------|
| 权限管控 | 告警配置仅 ADMIN 角色可见和操作 |
| 数据库兼容 | JPA `ddl-auto: update` 自动建表，兼容 MySQL 5.7+ |
| 优雅降级 | 告警服务异常不影响任务调度执行 |
| 飞书签名 | 飞书加签与钉钉加签使用不同的 HMAC-SHA256 算法，请勿混用 |
| 钉钉关键词 | 若机器人设置了关键词安全校验，必须在配置中填写对应关键词 |
