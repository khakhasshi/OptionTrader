# data/replay

本目录保存本地 Parquet 回放文件（ThetaData 原始/标准化高频行情、可重放事件流）。

**这些文件不提交到 Git**（见根 `.gitignore`）。PostgreSQL 保存数据集清单、路径、checksum、覆盖时段和导入状态，保证回放可追溯。

分区约定：`provider/data_type/symbol/trading_date/hour`。
