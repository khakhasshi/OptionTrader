import { useMemo, useState, type ReactNode } from "react";
import {
  Bot,
  CheckCircle2,
  Clipboard,
  Database,
  Eye,
  EyeOff,
  KeyRound,
  LockKeyhole,
  RefreshCcw,
  Router,
  Server,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Waypoints,
} from "lucide-react";

type SettingsTab = "runtime" | "connections" | "risk";

export interface OperatorSettings {
  environment: "local" | "replay" | "shadow" | "paper";
  marketSource: "replay" | "theta-sdk";
  executionBackend: "disabled" | "simulated-paper" | "ibkr-paper" | "longbridge-paper";
  streamSilenceSeconds: string;
  eventDirectory: string;
  thetaCredentialsFile: string;
  thetaDotenvPath: string;
  thetaApiKey: string;
  thetaGrpc: string;
  longbridgeAppKey: string;
  longbridgeAppSecret: string;
  longbridgeAccessToken: string;
  longbridgePaper: boolean;
  ibkrMode: "GATEWAY" | "TWS";
  ibkrHost: string;
  ibkrPort: string;
  ibkrClientId: string;
  ibkrAccount: string;
  ibkrPaper: boolean;
  ibkrSubmissionEnabled: boolean;
  reconciliationEnabled: boolean;
  reconciliationBroker: "ibkr" | "longbridge";
  reconciliationInterval: string;
  paperSubmissionEnabled: boolean;
  databaseUrl: string;
  confirmationFernetKey: string;
  riskLimitsConfirmed: boolean;
  ruleVersion: string;
  killSwitch: boolean;
  maxPlanLoss: string;
  maxDailyLoss: string;
  maxOpenRisk: string;
  maxDailyTrades: string;
  maxContracts: string;
  llmProvider: string;
  llmBaseUrl: string;
  llmApiKey: string;
  llmModel: string;
  llmTimeoutSeconds: string;
  llmMaxOutputTokens: string;
  llmDailyBudget: string;
  llmEnabled: boolean;
}

export const INITIAL_SETTINGS: OperatorSettings = {
  environment: "local",
  marketSource: "replay",
  executionBackend: "simulated-paper",
  streamSilenceSeconds: "90",
  eventDirectory: "../../data/events",
  thetaCredentialsFile: "",
  thetaDotenvPath: "",
  thetaApiKey: "",
  thetaGrpc: "http://127.0.0.1:50052",
  longbridgeAppKey: "",
  longbridgeAppSecret: "",
  longbridgeAccessToken: "",
  longbridgePaper: false,
  ibkrMode: "GATEWAY",
  ibkrHost: "127.0.0.1",
  ibkrPort: "",
  ibkrClientId: "37",
  ibkrAccount: "",
  ibkrPaper: true,
  ibkrSubmissionEnabled: false,
  reconciliationEnabled: true,
  reconciliationBroker: "ibkr",
  reconciliationInterval: "30",
  paperSubmissionEnabled: false,
  databaseUrl: "postgresql://optiontrader:optiontrader@localhost:5432/optiontrader",
  confirmationFernetKey: "",
  riskLimitsConfirmed: false,
  ruleVersion: "UNCONFIRMED",
  killSwitch: false,
  maxPlanLoss: "250",
  maxDailyLoss: "500",
  maxOpenRisk: "500",
  maxDailyTrades: "3",
  maxContracts: "2",
  llmProvider: "deepseek-openai",
  llmBaseUrl: "https://api.deepseek.com",
  llmApiKey: "",
  llmModel: "deepseek-v4-flash",
  llmTimeoutSeconds: "8",
  llmMaxOutputTokens: "1200",
  llmDailyBudget: "1.00",
  llmEnabled: false,
};

export function validateSettings(settings: OperatorSettings): string[] {
  const issues: string[] = [];
  if (Object.values(settings).some((value) => typeof value === "string" && /[\0\r\n]/.test(value))) {
    issues.push("环境变量值不能包含换行或 NUL 字符。");
  }
  const externalBroker =
    settings.executionBackend === "ibkr-paper"
      ? "ibkr"
      : settings.executionBackend === "longbridge-paper"
        ? "longbridge"
        : null;

  if (settings.marketSource === "theta-sdk") {
    const hasThetaAuth = Boolean(
      settings.thetaCredentialsFile || settings.thetaDotenvPath || settings.thetaApiKey,
    );
    if (!hasThetaAuth) issues.push("ThetaData SDK 需要配置一种凭证来源。");
  }
  if (externalBroker) {
    if (settings.environment !== "paper") issues.push("外部券商路由要求使用 paper 运行环境。");
    if (!settings.paperSubmissionEnabled) issues.push("外部券商路由要求开启全局模拟盘提交授权。");
    if (!settings.reconciliationEnabled || settings.reconciliationBroker !== externalBroker) {
      issues.push(`执行路由与持续对账必须使用同一券商（${externalBroker}）。`);
    }
  }
  if (settings.executionBackend === "ibkr-paper") {
    if (!settings.ibkrPaper || !settings.ibkrSubmissionEnabled) {
      issues.push("IBKR 模拟盘路由要求同时开启模拟盘与提交开关。");
    }
  }
  if (settings.executionBackend === "longbridge-paper" && !settings.longbridgePaper) {
    issues.push("Longbridge 模拟盘路由要求开启 Longbridge 模拟盘开关。");
  }
  if (settings.riskLimitsConfirmed && settings.ruleVersion.trim() === "UNCONFIRMED") {
    issues.push("确认风控限额前，必须填写已复核的规则版本。");
  }
  if (settings.llmEnabled) {
    if (!settings.llmProvider || !settings.llmBaseUrl || !settings.llmApiKey || !settings.llmModel) {
      issues.push("启用 LLM 前必须填写服务商、Base URL、API 密钥和模型。");
    }
    if (!settings.llmBaseUrl.startsWith("https://")) issues.push("LLM Base URL 必须使用 HTTPS。");
    const timeout = Number(settings.llmTimeoutSeconds);
    const outputTokens = Number(settings.llmMaxOutputTokens);
    const dailyBudget = Number(settings.llmDailyBudget);
    if (!Number.isFinite(timeout) || timeout < 1 || timeout > 30) issues.push("LLM 超时必须在 1–30 秒之间。");
    if (!Number.isInteger(outputTokens) || outputTokens < 128 || outputTokens > 4096) issues.push("LLM 最大输出必须是 128–4096 的整数。");
    if (!Number.isFinite(dailyBudget) || dailyBudget < 0.01) issues.push("LLM 单日预算不得低于 0.01 美元。");
  }
  for (const [label, value] of [
    ["行情流静默阈值", settings.streamSilenceSeconds],
    ["对账间隔", settings.reconciliationInterval],
    ["单计划最大亏损", settings.maxPlanLoss],
    ["单日最大亏损", settings.maxDailyLoss],
    ["最大未平仓风险", settings.maxOpenRisk],
    ["单日最大交易次数", settings.maxDailyTrades],
    ["最大合约张数", settings.maxContracts],
    ["LLM 超时", settings.llmTimeoutSeconds],
    ["LLM 最大输出 Token", settings.llmMaxOutputTokens],
    ["LLM 单日预算", settings.llmDailyBudget],
  ]) {
    if (!Number.isFinite(Number(value)) || Number(value) < 0) issues.push(`${label}必须是非负数。`);
  }
  return issues;
}

export function settingsToEnv(settings: OperatorSettings): string {
  if (Object.values(settings).some((value) => typeof value === "string" && /[\0\r\n]/.test(value))) {
    throw new Error("unsafe environment value");
  }
  const bool = (value: boolean) => (value ? "true" : "false");
  return [
    `OPTIONTRADER_ENV=${settings.environment}`,
    "LIVE_TRADING_ENABLED=false",
    `OPTIONTRADER_MARKET_SOURCE=${settings.marketSource}`,
    `OPTIONTRADER_STREAM_SILENCE_SECONDS=${settings.streamSilenceSeconds}`,
    `OPTIONTRADER_EVENT_DIR=${settings.eventDirectory}`,
    `THETADATA_CREDENTIALS_FILE=${settings.thetaCredentialsFile}`,
    `THETADATA_DOTENV_PATH=${settings.thetaDotenvPath}`,
    `THETADATA_API_KEY=${settings.thetaApiKey}`,
    `THETADATA_SDK_GRPC=${settings.thetaGrpc}`,
    `LONGBRIDGE_APP_KEY=${settings.longbridgeAppKey}`,
    `LONGBRIDGE_APP_SECRET=${settings.longbridgeAppSecret}`,
    `LONGBRIDGE_ACCESS_TOKEN=${settings.longbridgeAccessToken}`,
    `OPTIONTRADER_LONGBRIDGE_PAPER=${bool(settings.longbridgePaper)}`,
    `OPTIONTRADER_IBKR_MODE=${settings.ibkrMode}`,
    `OPTIONTRADER_IBKR_HOST=${settings.ibkrHost}`,
    `OPTIONTRADER_IBKR_PORT=${settings.ibkrPort}`,
    `OPTIONTRADER_IBKR_CLIENT_ID=${settings.ibkrClientId}`,
    `OPTIONTRADER_IBKR_ACCOUNT=${settings.ibkrAccount}`,
    `OPTIONTRADER_IBKR_PAPER=${bool(settings.ibkrPaper)}`,
    `OPTIONTRADER_IBKR_SUBMISSION_ENABLED=${bool(settings.ibkrSubmissionEnabled)}`,
    `OPTIONTRADER_BROKER_RECONCILIATION_ENABLED=${bool(settings.reconciliationEnabled)}`,
    `OPTIONTRADER_BROKER_RECONCILIATION_INTERVAL_SECONDS=${settings.reconciliationInterval}`,
    `OPTIONTRADER_BROKER_RECONCILIATION_BROKERS=${settings.reconciliationBroker}`,
    `OPTIONTRADER_BROKER_EXECUTION_BACKEND=${settings.executionBackend}`,
    `OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED=${bool(settings.paperSubmissionEnabled)}`,
    `DATABASE_URL=${settings.databaseUrl}`,
    `OPTIONTRADER_CONFIRMATION_FERNET_KEY=${settings.confirmationFernetKey}`,
    `OPTIONTRADER_RISK_LIMITS_CONFIRMED=${bool(settings.riskLimitsConfirmed)}`,
    `OPTIONTRADER_RULE_VERSION=${settings.ruleVersion}`,
    `OPTIONTRADER_KILL_SWITCH=${bool(settings.killSwitch)}`,
    `OPTIONTRADER_MAX_PLAN_LOSS=${settings.maxPlanLoss}`,
    `OPTIONTRADER_MAX_DAILY_LOSS=${settings.maxDailyLoss}`,
    `OPTIONTRADER_MAX_OPEN_RISK=${settings.maxOpenRisk}`,
    `OPTIONTRADER_MAX_DAILY_TRADES=${settings.maxDailyTrades}`,
    `OPTIONTRADER_MAX_CONTRACTS=${settings.maxContracts}`,
    `LLM_PROVIDER=${settings.llmEnabled ? settings.llmProvider : ""}`,
    `LLM_BASE_URL=${settings.llmEnabled ? settings.llmBaseUrl : ""}`,
    `LLM_API_KEY=${settings.llmEnabled ? settings.llmApiKey : ""}`,
    `LLM_MODEL=${settings.llmEnabled ? settings.llmModel : ""}`,
    `LLM_TIMEOUT_SECONDS=${settings.llmTimeoutSeconds}`,
    `LLM_MAX_OUTPUT_TOKENS=${settings.llmMaxOutputTokens}`,
    `LLM_DAILY_MAX_ESTIMATED_USD=${settings.llmDailyBudget}`,
  ].join("\n");
}

export function SettingsPage() {
  const [settings, setSettings] = useState<OperatorSettings>(INITIAL_SETTINGS);
  const [tab, setTab] = useState<SettingsTab>("runtime");
  const [checked, setChecked] = useState(false);
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");
  const issues = useMemo(() => validateSettings(settings), [settings]);

  const update = <K extends keyof OperatorSettings>(key: K, value: OperatorSettings[K]) => {
    setSettings((current) => ({ ...current, [key]: value }));
    setChecked(false);
    setCopyState("idle");
  };

  const copyEnvironment = async () => {
    if (issues.length > 0) {
      setChecked(true);
      setCopyState("failed");
      return;
    }
    try {
      await navigator.clipboard.writeText(settingsToEnv(settings));
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
  };

  const reset = () => {
    setSettings(INITIAL_SETTINGS);
    setChecked(false);
    setCopyState("idle");
  };

  return (
    <main className="page settings-page">
      <header className="page-header">
        <div>
          <span className="page-eyebrow"><SlidersHorizontal size={14} /> 操作员配置</span>
          <h1>系统设置</h1>
          <p>在不削弱执行闸门的前提下准备运行环境变量。</p>
        </div>
        <div className="header-actions">
          <button className="secondary-command" type="button" onClick={reset}>
            <RefreshCcw size={16} aria-hidden="true" /> 重置
          </button>
          <button className="primary-command" type="button" onClick={() => void copyEnvironment()}>
            <Clipboard size={16} aria-hidden="true" /> {copyState === "copied" ? "已复制" : "复制 .env"}
          </button>
        </div>
      </header>

      <div className="security-notice" role="note">
        <LockKeyhole size={18} aria-hidden="true" />
        <div>
          <strong>仅用于本地草拟</strong>
          <span>填写内容只保留在当前标签页内存中，不会发送到 API，也不会写入浏览器存储。</span>
        </div>
      </div>

      <div className="settings-tabs" role="tablist" aria-label="设置分类">
        <TabButton active={tab === "runtime"} onClick={() => setTab("runtime")} icon={<Router size={16} />}>运行与路由</TabButton>
        <TabButton active={tab === "connections"} onClick={() => setTab("connections")} icon={<Waypoints size={16} />}>外部连接</TabButton>
        <TabButton active={tab === "risk"} onClick={() => setTab("risk")} icon={<ShieldCheck size={16} />}>风控与 AI</TabButton>
      </div>

      {tab === "runtime" && (
        <div className="settings-grid" role="tabpanel">
          <SettingsSection icon={<Server size={18} />} title="运行环境" subtitle="环境模式与权威数据源">
            <SelectField label="环境模式" value={settings.environment} onChange={(value) => update("environment", value as OperatorSettings["environment"])}>
              <option value="local">local</option><option value="replay">replay</option><option value="shadow">shadow</option><option value="paper">paper</option>
            </SelectField>
            <SelectField label="行情来源" value={settings.marketSource} onChange={(value) => update("marketSource", value as OperatorSettings["marketSource"])}>
              <option value="replay">历史回放</option><option value="theta-sdk">ThetaData Python SDK</option>
            </SelectField>
            <TextField label="行情流静默阈值" suffix="秒" value={settings.streamSilenceSeconds} onChange={(value) => update("streamSilenceSeconds", value)} />
            <TextField label="事件文档目录" value={settings.eventDirectory} onChange={(value) => update("eventDirectory", value)} />
            <ReadOnlyField label="实盘交易" value="Phase 3 强制关闭" />
          </SettingsSection>

          <SettingsSection icon={<Router size={18} />} title="执行路由" subtitle="外部模拟盘提交必须满足全部联动闸门">
            <SelectField label="执行后端" value={settings.executionBackend} onChange={(value) => update("executionBackend", value as OperatorSettings["executionBackend"])}>
              <option value="disabled">关闭</option><option value="simulated-paper">内置模拟撮合</option><option value="ibkr-paper">IBKR 模拟盘</option><option value="longbridge-paper">Longbridge 模拟盘</option>
            </SelectField>
            <ToggleField label="全局模拟盘提交授权" detail="任何外部券商写操作都必须开启" checked={settings.paperSubmissionEnabled} onChange={(value) => update("paperSubmissionEnabled", value)} />
            <ToggleField label="持续对账" detail="持续同步券商账户事实" checked={settings.reconciliationEnabled} onChange={(value) => update("reconciliationEnabled", value)} />
            <SelectField label="对账券商" value={settings.reconciliationBroker} onChange={(value) => update("reconciliationBroker", value as OperatorSettings["reconciliationBroker"])}>
              <option value="ibkr">IBKR</option><option value="longbridge">Longbridge</option>
            </SelectField>
            <TextField label="对账间隔" suffix="秒" value={settings.reconciliationInterval} onChange={(value) => update("reconciliationInterval", value)} />
          </SettingsSection>
        </div>
      )}

      {tab === "connections" && (
        <div className="settings-grid" role="tabpanel">
          <SettingsSection icon={<Database size={18} />} title="ThetaData" subtitle="通过官方 Python SDK 提供权威行情">
            <TextField label="凭证文件" placeholder="/secure/path/creds.txt" value={settings.thetaCredentialsFile} onChange={(value) => update("thetaCredentialsFile", value)} />
            <TextField label="Dotenv 路径" placeholder="/secure/path/.env" value={settings.thetaDotenvPath} onChange={(value) => update("thetaDotenvPath", value)} />
            <SecretField label="API 密钥" value={settings.thetaApiKey} onChange={(value) => update("thetaApiKey", value)} />
            <TextField label="SDK gRPC 地址" value={settings.thetaGrpc} onChange={(value) => update("thetaGrpc", value)} />
          </SettingsSection>

          <SettingsSection icon={<KeyRound size={18} />} title="Longbridge" subtitle="模拟盘券商适配器凭证">
            <SecretField label="应用 Key" value={settings.longbridgeAppKey} onChange={(value) => update("longbridgeAppKey", value)} />
            <SecretField label="应用 Secret" value={settings.longbridgeAppSecret} onChange={(value) => update("longbridgeAppSecret", value)} />
            <SecretField label="访问令牌" value={settings.longbridgeAccessToken} onChange={(value) => update("longbridgeAccessToken", value)} />
            <ToggleField label="Longbridge 模拟盘" detail="此开关不会启用实盘路由" checked={settings.longbridgePaper} onChange={(value) => update("longbridgePaper", value)} />
          </SettingsSection>

          <SettingsSection icon={<Server size={18} />} title="IBKR" subtitle="TWS 或 Gateway 边车连接">
            <SelectField label="客户端模式" value={settings.ibkrMode} onChange={(value) => update("ibkrMode", value as OperatorSettings["ibkrMode"])}>
              <option value="GATEWAY">Gateway</option><option value="TWS">TWS</option>
            </SelectField>
            <div className="form-row-split">
              <TextField label="主机" value={settings.ibkrHost} onChange={(value) => update("ibkrHost", value)} />
              <TextField label="端口" placeholder="自动" value={settings.ibkrPort} onChange={(value) => update("ibkrPort", value)} />
            </div>
            <div className="form-row-split">
              <TextField label="客户端 ID" value={settings.ibkrClientId} onChange={(value) => update("ibkrClientId", value)} />
              <TextField label="账户" value={settings.ibkrAccount} onChange={(value) => update("ibkrAccount", value)} />
            </div>
            <ToggleField label="IBKR 模拟盘账户" detail="自动采用模拟盘默认端口" checked={settings.ibkrPaper} onChange={(value) => update("ibkrPaper", value)} />
            <ToggleField label="允许 IBKR 提交" detail="券商级写操作授权" checked={settings.ibkrSubmissionEnabled} onChange={(value) => update("ibkrSubmissionEnabled", value)} />
          </SettingsSection>

          <SettingsSection icon={<Database size={18} />} title="持久化" subtitle="PostgreSQL 与确认权限加密">
            <SecretField label="数据库 URL" value={settings.databaseUrl} onChange={(value) => update("databaseUrl", value)} />
            <SecretField label="Fernet 密钥环" value={settings.confirmationFernetKey} onChange={(value) => update("confirmationFernetKey", value)} />
          </SettingsSection>
        </div>
      )}

      {tab === "risk" && (
        <div className="settings-grid" role="tabpanel">
          <SettingsSection icon={<ShieldAlert size={18} />} title="权威风控" subtitle="由 Rust 网关强制执行的启动限额">
            <ToggleField label="紧急停止开关" detail="立即阻止新增风险" checked={settings.killSwitch} onChange={(value) => update("killSwitch", value)} danger />
            <ToggleField label="限额已复核" detail="操作员签字确认前不得开启" checked={settings.riskLimitsConfirmed} onChange={(value) => update("riskLimitsConfirmed", value)} />
            <TextField label="规则版本" value={settings.ruleVersion} onChange={(value) => update("ruleVersion", value)} />
            <div className="form-row-split">
              <TextField label="单计划最大亏损" prefix="$" value={settings.maxPlanLoss} onChange={(value) => update("maxPlanLoss", value)} />
              <TextField label="单日最大亏损" prefix="$" value={settings.maxDailyLoss} onChange={(value) => update("maxDailyLoss", value)} />
            </div>
            <TextField label="最大未平仓风险" prefix="$" value={settings.maxOpenRisk} onChange={(value) => update("maxOpenRisk", value)} />
            <div className="form-row-split">
              <TextField label="单日交易次数" value={settings.maxDailyTrades} onChange={(value) => update("maxDailyTrades", value)} />
              <TextField label="最大合约张数" value={settings.maxContracts} onChange={(value) => update("maxContracts", value)} />
            </div>
          </SettingsSection>

          <SettingsSection icon={<Bot size={18} />} title="LLM 智能顾问" subtitle="异步解释与复盘，永不拥有执行权限">
            <ToggleField label="启用 LLM 审阅" detail="仅开放独立审阅 API" checked={settings.llmEnabled} onChange={(value) => update("llmEnabled", value)} />
            <TextField label="服务商" placeholder="服务商名称" value={settings.llmProvider} onChange={(value) => update("llmProvider", value)} />
            <TextField label="Base URL" placeholder="https://api.example.com" value={settings.llmBaseUrl} onChange={(value) => update("llmBaseUrl", value)} />
            <SecretField label="LLM API 密钥" value={settings.llmApiKey} onChange={(value) => update("llmApiKey", value)} />
            <TextField label="模型" placeholder="模型标识" value={settings.llmModel} onChange={(value) => update("llmModel", value)} />
            <div className="form-row-split">
              <TextField label="超时" suffix="秒" value={settings.llmTimeoutSeconds} onChange={(value) => update("llmTimeoutSeconds", value)} />
              <TextField label="最大输出" suffix="tokens" value={settings.llmMaxOutputTokens} onChange={(value) => update("llmMaxOutputTokens", value)} />
            </div>
            <TextField label="单日成本上限" prefix="$" value={settings.llmDailyBudget} onChange={(value) => update("llmDailyBudget", value)} />
            <ReadOnlyField label="权限边界" value="仅提供建议" />
          </SettingsSection>
        </div>
      )}

      <section className={`validation-bar${checked ? " checked" : ""}`} aria-live="polite">
        <div className="validation-summary">
          {checked && issues.length === 0 ? <CheckCircle2 size={18} /> : <ShieldAlert size={18} />}
          <div>
            <strong>{checked ? (issues.length === 0 ? "配置内部一致" : `发现 ${issues.length} 项闸门问题`) : "导出前请运行预检"}</strong>
            <span>{copyState === "failed" ? "无法访问剪贴板，请检查浏览器权限。" : "校验不会启用实盘，也不会把参数应用到正在运行的服务。"}</span>
          </div>
        </div>
        {checked && issues.length > 0 && <ul>{issues.map((issue) => <li key={issue}>{issue}</li>)}</ul>}
        <button className="secondary-command" type="button" onClick={() => setChecked(true)}>
          <ShieldCheck size={16} aria-hidden="true" /> 校验配置
        </button>
      </section>
    </main>
  );
}

function TabButton({ active, onClick, icon, children }: { active: boolean; onClick: () => void; icon: ReactNode; children: ReactNode }) {
  return <button type="button" role="tab" aria-selected={active} className={active ? "active" : ""} onClick={onClick}>{icon}{children}</button>;
}

function SettingsSection({ icon, title, subtitle, children }: { icon: ReactNode; title: string; subtitle: string; children: ReactNode }) {
  return <section className="settings-section"><header><span className="section-icon">{icon}</span><div><h2>{title}</h2><p>{subtitle}</p></div></header><div className="settings-fields">{children}</div></section>;
}

function TextField({ label, value, onChange, placeholder, prefix, suffix }: { label: string; value: string; onChange: (value: string) => void; placeholder?: string; prefix?: string; suffix?: string }) {
  return <label className="control-field"><span>{label}</span><div className="input-shell">{prefix && <b>{prefix}</b>}<input value={value} placeholder={placeholder} onChange={(event) => onChange(event.target.value)} />{suffix && <small>{suffix}</small>}</div></label>;
}

function SecretField({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  const [visible, setVisible] = useState(false);
  return <label className="control-field"><span>{label}</span><div className="input-shell secret-input"><input aria-label={label} type={visible ? "text" : "password"} autoComplete="new-password" value={value} onChange={(event) => onChange(event.target.value)} /><button type="button" title={`${visible ? "隐藏" : "显示"}${label}`} onClick={() => setVisible((current) => !current)}>{visible ? <EyeOff size={16} /> : <Eye size={16} />}</button></div></label>;
}

function SelectField({ label, value, onChange, children }: { label: string; value: string; onChange: (value: string) => void; children: ReactNode }) {
  return <label className="control-field"><span>{label}</span><div className="input-shell"><select value={value} onChange={(event) => onChange(event.target.value)}>{children}</select></div></label>;
}

function ToggleField({ label, detail, checked, onChange, danger = false }: { label: string; detail: string; checked: boolean; onChange: (value: boolean) => void; danger?: boolean }) {
  return <label className={`toggle-row${danger ? " danger" : ""}`}><span><strong>{label}</strong><small>{detail}</small></span><input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} /><i aria-hidden="true" /></label>;
}

function ReadOnlyField({ label, value }: { label: string; value: string }) {
  return <div className="readonly-field"><span>{label}</span><strong>{value}</strong></div>;
}
