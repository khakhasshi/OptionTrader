# OptionTrader — unified command entry point
# JS: npm workspaces | Python: uv | Rust: cargo | DB: PostgreSQL + Alembic

.DEFAULT_GOAL := help
.PHONY: help setup setup-web setup-api setup-core dev dev-web dev-api dev-core \
        health test test-contracts test-web test-api test-core lint lint-web lint-api lint-core \
        contracts migrate migrate-down up down clean

WEB_DIR  := apps/web
API_DIR  := services/application-api
CORE_DIR := services/trading-core

help: ## 列出所有可用命令
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup: setup-web setup-api setup-core ## 安装三端依赖

setup-web: ## 安装前端依赖
	npm install

setup-api: ## 安装 Python 依赖 (uv)
	cd $(API_DIR) && uv sync

setup-core: ## 拉取并编译 Rust 依赖
	cd $(CORE_DIR) && cargo fetch

dev: ## 本地并行启动 web / application-api / trading-core
	@echo "分别在独立终端运行: make dev-web / make dev-api / make dev-core"

dev-web: ## 启动 React 驾驶舱
	npm --workspace $(WEB_DIR) run dev

dev-api: ## 启动 Python FastAPI
	cd $(API_DIR) && uv run uvicorn app.main:app --reload --port 8000

dev-core: ## 启动 Rust trading-core
	cd $(CORE_DIR) && cargo run

health: ## 检查三个服务 health endpoint
	@echo "web:  http://localhost:5173"
	@curl -sf http://localhost:8000/api/v1/health && echo " <- application-api OK" || echo "application-api DOWN"
	@curl -sf http://localhost:8080/health && echo " <- trading-core OK" || echo "trading-core DOWN"

test: test-contracts test-web test-api test-core ## 运行契约 + 三端单元测试

test-contracts: ## 校验 JSON Schema 契约与 fixtures
	cd $(API_DIR) && uv run --with jsonschema pytest ../../tests/contract -q

test-web: ## 前端测试 (Phase 0: typecheck + build)
	npm --workspace $(WEB_DIR) run test --if-present
	npm --workspace $(WEB_DIR) run build

test-api: ## Python 测试
	cd $(API_DIR) && uv run pytest

test-core: ## Rust 测试
	cd $(CORE_DIR) && cargo test

lint: lint-web lint-api lint-core ## 三端 lint + format check

lint-web: ## 前端 lint + typecheck
	npm --workspace $(WEB_DIR) run lint --if-present

lint-api: ## Python lint + typecheck
	cd $(API_DIR) && uv run ruff check . && uv run mypy .

lint-core: ## Rust fmt + clippy
	cd $(CORE_DIR) && cargo fmt --check && cargo clippy -- -D warnings

contracts: ## 生成 Protobuf / JSON Schema / OpenAPI 客户端
	bash scripts/gen_contracts.sh

migrate: ## Alembic 迁移到最新 (可用 DATABASE_URL 覆盖目标库)
	cd $(API_DIR) && uv run alembic upgrade head

migrate-down: ## 回滚全部迁移 (仅开发环境)
	cd $(API_DIR) && uv run alembic downgrade base

up: ## 启动本地依赖 (PostgreSQL 等)
	docker compose -f infra/compose/docker-compose.yml up -d

down: ## 停止本地依赖
	docker compose -f infra/compose/docker-compose.yml down

clean: ## 清理构建产物
	rm -rf node_modules dist $(WEB_DIR)/dist $(CORE_DIR)/target
