import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import {
  INITIAL_SETTINGS,
  SettingsPage,
  settingsToEnv,
  validateSettings,
} from "./SettingsPage";

afterEach(cleanup);

describe("SettingsPage security posture", () => {
  it("keeps secret values masked until the operator reveals them", () => {
    render(<SettingsPage />);
    fireEvent.click(screen.getByRole("tab", { name: "外部连接" }));
    const apiKey = screen.getByLabelText("API 密钥");
    expect(apiKey).toHaveAttribute("type", "password");
    fireEvent.click(screen.getByTitle("显示API 密钥"));
    expect(apiKey).toHaveAttribute("type", "text");
  });

  it("states that draft values stay in tab memory", () => {
    render(<SettingsPage />);
    expect(screen.getByText(/不会发送到 API，也不会写入浏览器存储/)).toBeInTheDocument();
  });
});

describe("settings validation", () => {
  it("fails closed when IBKR routing is selected without all paper gates", () => {
    const issues = validateSettings({ ...INITIAL_SETTINGS, executionBackend: "ibkr-paper" });
    expect(issues).toContain("外部券商路由要求使用 paper 运行环境。");
    expect(issues).toContain("外部券商路由要求开启全局模拟盘提交授权。");
    expect(issues).toContain("IBKR 模拟盘路由要求同时开启模拟盘与提交开关。");
  });

  it("accepts a fully linked IBKR paper configuration", () => {
    const issues = validateSettings({
      ...INITIAL_SETTINGS,
      environment: "paper",
      executionBackend: "ibkr-paper",
      paperSubmissionEnabled: true,
      reconciliationEnabled: true,
      reconciliationBroker: "ibkr",
      ibkrPaper: true,
      ibkrSubmissionEnabled: true,
    });
    expect(issues).toEqual([]);
  });

  it("requires ThetaData credentials when the SDK source is active", () => {
    expect(validateSettings({ ...INITIAL_SETTINGS, marketSource: "theta-sdk" })).toContain(
      "ThetaData SDK 需要配置一种凭证来源。",
    );
  });
});

describe("environment export", () => {
  it("pins live trading off and includes the selected safe backend", () => {
    const output = settingsToEnv({
      ...INITIAL_SETTINGS,
      environment: "paper",
      executionBackend: "simulated-paper",
    });
    expect(output).toContain("LIVE_TRADING_ENABLED=false");
    expect(output).toContain("OPTIONTRADER_BROKER_EXECUTION_BACKEND=simulated-paper");
    expect(output).not.toContain("LIVE_TRADING_ENABLED=true");
  });

  it("exports a complete enabled LLM provider without granting trading authority", () => {
    const output = settingsToEnv({
      ...INITIAL_SETTINGS,
      llmEnabled: true,
      llmApiKey: "draft-only-key",
    });
    expect(output).toContain("LLM_PROVIDER=deepseek-openai");
    expect(output).toContain("LLM_BASE_URL=https://api.deepseek.com");
    expect(output).toContain("LLM_MODEL=deepseek-v4-flash");
    expect(output).not.toContain("LLM_TRADING_AUTHORITY");
  });

  it("rejects environment-line injection during export", () => {
    expect(() => settingsToEnv({
      ...INITIAL_SETTINGS,
      llmEnabled: true,
      llmApiKey: "draft-key\nLIVE_TRADING_ENABLED=true",
    })).toThrow("unsafe environment value");
  });
});
