# 历史档案混合编码归一与索引保形

        面向一线复核人员的单服务数据工作台。样例数据随仓库提供，结论保留行号、原因码和原值。

        ## 业务模块

        - M1 文件采信：`/api/intake/run`
- M2 编码判定：`/api/encoding/detect`
- M3 字段保形：`/api/fields/normalize`
- M4 内容归一：`/api/content/normalize`
- M5 索引对账：`/api/reconcile`
- M6 批次导出：`/api/batches`（入库）· `/api/batches/{id}/revoke`（撤回）· `/api/batches/{id}/purge`（清除）· `/api/batches/{id}/audit`（审计）· `/api/export/evidence.csv`

        ## 核心规则

        候选解码必须完整且无替换字符；档号、盒号与超大流水号始终按字符串逐字保留。

        ## 启动

        ```bash
        docker compose up --build
        ```

        浏览器访问 `http://localhost:18210`。运行测试：

        ```bash
        docker compose exec -T app pytest -q
        ```

        ## 样例流程

        依次运行六个模块；最后在批次页复核输入、有效、隔离与输出合计。同一输入摘要重复执行时复用原 `run_id`。

        批次页支持档案记录入库、按批次撤回与审计查看：撤回即刻生效，档号与原始内容不再可读，
        计数、行号与处置理由留账供对账；清除分块执行，中断重启后可续跑，重试不重复记账，
        旧采信请求重放不会复活已撤回记录；证据导出实时反映库内状态。

        ## 已知不覆盖

        不处理加密文件与需要外部密钥的对象；不连接真实生产存储。
