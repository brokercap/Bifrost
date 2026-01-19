# Feature: Support MySQL to SAP HANA Data Synchronization (支持 MySQL 到 SAP HANA 数据同步)

## 🇨🇳 中文详细说明 (Chinese Description)

### 1. 摘要 (Summary)
本 PR 引入了一个全新的插件 `hana`，旨在支持从 MySQL 到 SAP HANA 的高性能实时数据同步，彻底解决了 issue #306。该插件基于 SAP 官方的 `go-hdb` 驱动开发，并针对 Bifrost 的架构实现了高效的事务性批量处理机制，能够满足企业级数据同步需求。

### 2. 核心功能与变更 (Key Features & Changes)

*   **原生驱动支持**: 新增 `plugin/hana` 目录，核心逻辑直接基于 `github.com/SAP/go-hdb/driver` 实现，无需中间件。
*   **全量 DML 支持**: 完整支持 `INSERT`、`UPDATE`、`DELETE` 事件的同步，并针对 HANA 语法进行了适配。
*   **智能批量处理**:
    *   内置可配置的事件缓冲池（`BatchSize`，默认 500）。
    *   能够自动将高频的小事务合并为批量事务提交，显著减少网络 RTT 和数据库 IOPS。
*   **高可靠性事务设计**:
    *   **按表分组**: 提交时自动将同表数据归类，每个表的数据在一个独立的 `Transition` 中执行。
    *   **原子性保障**: 使用 `Begin()` 和 `Commit()`/`Rollback()` 确保批次数据的完整性，任一记录失败均会触发回滚并报错，避免数据不一致。
*   **健壮的类型映射**:
    *   自动处理 Go 语言与 HANA 数据库的类型转换。
    *   复杂结构（如 JSON、Map、Slice）自动序列化为字符串存储。
    *   支持 `json.Number` 高精度数值。

### 3. 配置指南 (Configuration)

在 Bifrost 管理界面添加目标库时，请使用以下格式的 URI：

```text
hdb://user:password@host:port?schema=TARGET_SCHEMA
```

**参数说明**:
*   `user`: HANA 数据库用户名
*   `password`: 密码
*   `host`: 数据库地址
*   `port`: 数据库端口 (通常为 3xx15)
*   `schema`: (可选) 指定同步到的目标 Schema。如果不填，默认使用源库名或 SQL 中的 Schema。

**高级参数 (Plugin Param)**:
*   `BatchSize`: 批量提交的大小。默认为 `500`。增大此值可提高吞吐量，但会增加延迟。

### 4. 测试与验证 (How to Test)

#### 4.1 单元测试 (Unit Test)
本项目包含完整的单元测试，用于验证 URI 解析、参数校验及基本逻辑。

```bash
# 在项目根目录下运行
go test -v ./plugin/hana/src/...
```

#### 4.2 集成测试 (Integration Test)
1.  **环境准备**: 准备一个可访问的 SAP HANA 实例。
2.  **Bifrost 配置**:
    *   启动 Bifrost。
    *   在 "To Server" 中添加 HANA 目标：`hdb://system:Password123@192.168.1.100:39015`。
    *   创建一个同步通道，选择 MySQL 源表和 HANA 目标表。
3.  **功能验证**:
    *   **Insert**: 在 MySQL 插入 1000 条数据，HANA 端应在数秒内可见（取决于 BatchSize）。
    *   **Update**: 更新 MySQL 某行数据，验证 HANA 端对应字段变更。
    *   **Delete**: 删除 MySQL 数据，验证 HANA 端数据消失。
    *   **重启测试**: 重启 Bifrost，验证断点续传功能是否正常。

### 5. 实现细节 (Implementation Details)

*   **文件结构**:
    *   `plugin/hana/hana.go`: 插件注册入口。
    *   `plugin/hana/src/hana.go`: 核心连接与同步逻辑实现。
    *   `plugin/hana/src/hana_test.go`: 单元测试文件。
    *   `plugin/hana/www/`: 插件 UI 文档静态资源。
*   **依赖管理**:
    *   `go.mod` 已更新，添加了 `github.com/SAP/go-hdb v1.14.18`。
    *   已执行 `go mod tidy` 并与上游保持一致。

---

[查看代码变更 (View Changes)](https://github.com/brokercap/Bifrost/compare/master...ljluestc:Bifrost:feature/hana-support?expand=1)
