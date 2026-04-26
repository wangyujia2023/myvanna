一个 CLAUDE.md 文件竟然拿下 1.7 万 star，它到底写了什么
2026-04-13 12:43·编程进阶社
GitHub 上有一个项目，没有复杂的框架，没有依赖，没有安装步骤，就一个 Markdown 文件，结果拿了 1.7 万颗 star。

这件事本身就值得停下来想一想。

项目叫 andrej-karpathy-skills ，作者 Forrest Chang。它的核心内容是一个 CLAUDE.md 文件，放进你的代码仓库，用来约束 Claude Code 的行为。规则源自 Andrej Karpathy 对 LLM 编程行为的公开观察，被整理成四条原则。


为什么是 Karpathy？

Karpathy 的特殊之处在于，他不是一个旁观者。他是 OpenAI 的联合创始人之一，是 GPT 时代最重要的推动者之一，后来又在特斯拉主导了 FSD 的 AI 架构。换句话说，他是亲手把这些模型做出来的人。当他说 LLM 在写代码时有系统性缺陷，这个判断的分量完全不同于一般用户的抱怨。

他在 X 上的原话是：

"The models make wrong assumptions on your behalf and just run along with them without checking. They don't manage their confusion, don't seek clarifications, don't surface inconsistencies, don't present tradeoffs, don't push back when they should."

"They really like to overcomplicate code and APIs, bloat abstractions, don't clean up dead code... implement a bloated construction over 1000 lines when 100 would do."

"They still sometimes change/remove comments and code they don't sufficiently understand as side effects, even if orthogonal to the task."

用过 AI 编程工具的人，大概都在这三段话里找到过自己的经历。

一个文件，四条规则

Forrest Chang 做的事情很克制。他没有开发插件，没有搞界面，没有写 SDK。就是把 Karpathy 的观察翻译成了四条可以直接塞进 CLAUDE.md 的行为准则，然后开源出来。

来看实际内容：

## 1. 先想后写（Think Before Coding）
**不要假设，不要藏着疑惑，把取舍摆出来。**
动手之前：- 明确说出你的假设，如果不确定，就问。- 如果存在多种理解，把它们列出来，不要自己悄悄选一个。- 如果有更简单的方案，说出来，推一下。- 如果有什么搞不清楚，停下来，说清楚哪里不明白，再问。## 2. 能简则简（Simplicity First）
**用最少的代码解决问题，不做多余的事。**
- 不加用户没要求的功能。- 不为一次性逻辑搭抽象层。- 不预加"灵活性"或"可配置性"。
- 不为不可能发生的场景写错误处理。- 如果写了 200 行但 50 行够用，重写。
自检标准：一个高级工程师看了会觉得过度设计吗？如果会，简化。## 3. 精准修改（Surgical Changes）
**只动你该动的地方，只清理你自己制造的烂摊子。**
修改已有代码时：- 不要"顺手优化"旁边的代码、注释或格式。
- 不要重构没有问题的逻辑。- 保持原有风格，即使你觉得可以写得更好。- 发现不相关的死代码，提一句，不要擅自删。当你的改动制造了孤儿代码时：- 清掉你的改动造成的多余 import、变量、函数。
- 原来就有的死代码，不要动，除非被要求。检验标准：每一处改动都能直接追溯到用户的请求。## 4. 目标驱动执行（Goal-Driven Execution）
**定义成功标准，循环直到验证通过。**
把指令转化成可验证的目标：- "加一个校验" → "先写覆盖非法输入的测试，再让测试通过"
- "修这个 bug" → "先写能复现 bug 的测试，再修"
- "重构 X" → "确保重构前后测试都通过"
多步骤任务，列出简要计划：
1. [步骤] → 验证：[检查项]
2. [步骤] → 验证：[检查项]
3. [步骤] → 验证：[检查项]