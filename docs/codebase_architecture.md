# myvanna 代码目录规划

目标：把智能问数、Metric Cube 治理、归因分析逐步整理成清晰的 `agent -> skill -> pipeline -> service` 分层，避免能力继续堆在单个文件里。

## 目标分层

```text
api/
  main.py                         # FastAPI 薄入口，只做请求/响应和依赖装配

vanna_skill/
  agents/                         # 有状态/可推理的决策节点
    intent_agent.py                # 意图识别
    query_plan_agent.py            # 查询计划/任务拆解
    recall_fusion_agent.py         # 多路召回融合
    sql_guard_agent.py             # 生成后 SQL 守卫

  skills/                         # 可复用工具能力，无全链路状态
    doris_schema_skill.py          # Schema/DDL 召回
    sql_example_skill.py           # Golden SQL 召回
    semantic_sql_rag_skill.py      # 参数泛化 SQL RAG
    lineage_skill.py               # 血缘召回
    audit_pattern_skill.py         # audit_log 模式挖掘

  pipelines/                      # 端到端编排，连接 agent 与 skill
    cube_pipeline.py               # 默认智能问数主链路
    langchain_pipeline.py          # LangChain 实验链路
    semantic_pipeline.py           # 旧语义层兼容链路，后续冻结

  cube/                           # Metric Cube 语义控制平面
    models.py                      # 配置数据结构
    store.py                       # Doris 配置仓库
    service.py                     # Cube SQL 编译/缓存入口
    renderer.py                    # Cube 模型文件渲染
    validator.py                   # 配置治理校验
    regression.py                  # 规划中：回归测试执行器
    publisher.py                   # 规划中：发布/版本管理

  retrieval/                      # 通用检索基础设施
    doris_knowledge_retriever.py

  semantic/                       # 旧 Semantic 实验能力
    semantic_sql_rag.py            # 保留给 Cube 的 Golden SQL RAG 复用

  core/                           # 横切能力
    security.py                    # 自然语言/SQL 安全守卫

  doris_client.py                 # Doris 连接封装
  config_store.py                 # 统一配置读取
  prompt_store.py                 # Prompt 版本存储
  tracer.py                       # Trace 持久化
```

## 迁移原则

1. `api/main.py` 不再沉淀业务逻辑，新功能优先落到 `vanna_skill/*` 模块。
2. `pipeline` 只负责编排，不直接写复杂 SQL、不直接维护大量配置规则。
3. `agent` 负责判断、规划、选择路径；`skill` 负责可复用的确定性能力。
4. Cube 是默认语义层，旧 `semantic/` 只保留兼容和 Golden SQL RAG 复用。
5. 每个治理能力都要有 Doris 表、后端 service、页面入口、trace/日志四件套。

## 下一步拆分顺序

1. 把 Cube 管理 CRUD 从 `api/main.py` 下沉到 `vanna_skill/cube/admin.py`。
2. 新增 `vanna_skill/cube/regression.py`，让回归测试直接使用默认 Cube 七层链路。
3. 新增 `vanna_skill/cube/publisher.py`，把校验通过、发布版本、同步缓存串成发布流。
4. 将归因分析放入独立目录 `vanna_skill/rca/`，复用 Cube 指标图谱与智能问数执行器。
