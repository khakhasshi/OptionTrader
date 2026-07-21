# OptionTrader — unified command entry point
# JS: npm workspaces | Python: uv | Rust: cargo | DB: PostgreSQL + Alembic

.DEFAULT_GOAL := help
.PHONY: help setup setup-web setup-api setup-core dev dev-web dev-api dev-core dev-core-theta \
        health build-core test test-contracts test-web test-api test-core test-integration \
        lint lint-web lint-api lint-core \
        contracts gen-py-grpc events-context migrate migrate-down up down clean

WEB_DIR  := apps/web
API_DIR  := services/application-api
CORE_DIR := services/trading-core

help: ## 列出所有可用命令
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup: setup-web setup-api setup-core ## 安装三端依赖

setup-web: ## 安装前端依赖
	npm install

setup-api: ## 安装 Python 依赖 (uv) + 生成 gRPC 桩
	cd $(API_DIR) && uv sync
	$(MAKE) gen-py-grpc

setup-core: ## 拉取并编译 Rust 依赖
	cd $(CORE_DIR) && cargo fetch

gen-py-grpc: ## 生成 Python gRPC 桩到 app/grpc_gen/ (git 忽略，必须可重建)
	bash scripts/gen_python_grpc.sh

dev: ## 本地并行启动 web / application-api / trading-core
	@echo "分别在独立终端运行: make dev-web / make dev-api / make dev-core"

dev-web: ## 启动 React 驾驶舱
	npm --workspace $(WEB_DIR) run dev

dev-api: gen-py-grpc ## 启动 Python FastAPI
	cd $(API_DIR) && uv run uvicorn app.main:app --reload --port 8000

dev-core: ## 启动 Rust trading-core (HTTP :8080 + gRPC :50051)
	cd $(CORE_DIR) && cargo run

dev-core-theta: ## 以 Theta Terminal 实时源启动 trading-core（含当日 REST 回补）
	cd $(CORE_DIR) && OPTIONTRADER_MARKET_SOURCE=theta cargo run

health: ## 检查三个服务 health endpoint
	@echo "web:  http://localhost:5173"
	@curl -sf http://localhost:8000/api/v1/health && echo " <- application-api OK" || echo "application-api DOWN"
	@curl -sf http://localhost:8080/health && echo " <- trading-core HTTP OK" || echo "trading-core HTTP DOWN"
	@echo "trading-core gRPC: localhost:50051 (StreamMarketSnapshots / GetDataHealth)"

build-core: ## 构建当前 Rust trading-core 二进制（供跨语言 smoke 使用）
	cd $(CORE_DIR) && cargo build --bin trading-core

test: test-contracts test-web test-api test-core test-integration ## 运行契约 + 三端测试 + 强制跨语言 smoke

test-contracts: ## 校验 JSON Schema 契约与 fixtures
	cd $(API_DIR) && uv run --with jsonschema pytest ../../tests/contract -q

test-web: ## 前端测试 (vitest 单元测试 + typecheck + build)
	npm --workspace $(WEB_DIR) run test --if-present
	npm --workspace $(WEB_DIR) run lint
	npm --workspace $(WEB_DIR) run build

test-api: gen-py-grpc ## Python 测试（跨语言 smoke 由 test-integration 单独执行）
	cd $(API_DIR) && uv run pytest --ignore=tests/test_integration_smoke.py

test-core: ## Rust 测试
	cd $(CORE_DIR) && cargo test

test-integration: gen-py-grpc build-core ## 强制执行当前 Rust 二进制→Python 的跨语言 smoke
	cd $(API_DIR) && OPTIONTRADER_REQUIRE_INTEGRATION=1 uv run pytest tests/test_integration_smoke.py -q

lint: lint-web lint-api lint-core ## 三端 lint + format check

lint-web: ## 前端 lint + typecheck
	npm --workspace $(WEB_DIR) run lint --if-present

lint-api: gen-py-grpc ## Python lint + format check + typecheck
	cd $(API_DIR) && uv run ruff check . && uv run ruff format --check . && uv run mypy .

lint-core: ## Rust fmt + clippy (all targets)
	cd $(CORE_DIR) && cargo fmt --check && cargo clippy --all-targets -- -D warnings

contracts: ## 生成 Protobuf / JSON Schema / OpenAPI 客户端
	bash scripts/gen_contracts.sh

events-context: ## 验证当前日期的四类事件输入并生成 EventContext
	cd $(API_DIR) && uv run python -m app.events.cli --event-dir ../../data/events

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
