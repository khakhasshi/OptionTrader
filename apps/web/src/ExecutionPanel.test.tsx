import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ExecutionPanel } from "./ExecutionPanel";
import { EXECUTION_TICKET_FIXTURE as TICKET } from "./executionTestFixture";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("ExecutionPanel", () => {
  it("requires current trading permission and exact-plan acknowledgement", async () => {
    const fetchMock = vi.fn(async (_url: string, options?: RequestInit) => {
      if (options?.method === "POST") {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            ...TICKET.order,
            state: "WORKING",
            state_version: 4,
            broker_order_id: "paper-1",
          }),
        } as Response;
      }
      return { ok: true, status: 200, json: async () => TICKET } as Response;
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<ExecutionPanel sessionId="live" canTrade />);

    const confirm = await screen.findByRole("button", { name: "Confirm" });
    expect(confirm).toBeEnabled();
    fireEvent.click(confirm);
    const finalButton = screen.getByRole("button", { name: "Confirm exact hash" });
    expect(finalButton).toBeDisabled();
    fireEvent.click(screen.getByRole("checkbox"));
    expect(finalButton).toBeEnabled();
    await act(async () => fireEvent.click(finalButton));
    await waitFor(() => expect(screen.getByText("WORKING")).toBeInTheDocument());
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/trading/orders/order-1/confirm",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ plan_hash: "a".repeat(64) }),
      }),
    );
  });

  it("keeps confirmation disabled when the cockpit gate is closed", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: true, status: 200, json: async () => TICKET }) as Response),
    );
    render(<ExecutionPanel sessionId="live" canTrade={false} />);
    expect(await screen.findByRole("button", { name: "Confirm" })).toBeDisabled();
  });

  it("fails closed on malformed or unavailable audit responses", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ bad: true }) }) as Response),
    );
    render(<ExecutionPanel sessionId="live" canTrade />);
    expect(await screen.findByText("Execution audit unavailable")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Confirm" })).not.toBeInTheDocument();
  });

  it("does not regress after a stale polling response", async () => {
    let getCount = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_url: string, options?: RequestInit) => {
        if (options?.method === "POST") {
          return {
            ok: true,
            status: 200,
            json: async () => ({
              ...TICKET.order,
              state: "WORKING",
              state_version: 4,
              broker_order_id: "paper-1",
              updated_at_utc: "2099-07-21T14:30:04Z",
            }),
          } as Response;
        }
        getCount += 1;
        return { ok: true, status: 200, json: async () => TICKET } as Response;
      }),
    );
    render(<ExecutionPanel sessionId="live" canTrade />);
    fireEvent.click(await screen.findByRole("button", { name: "Confirm" }));
    fireEvent.click(screen.getByRole("checkbox"));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Confirm exact hash" })));
    await waitFor(() => expect(screen.getByText("WORKING")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: "Refresh execution state" }));
    await waitFor(() => expect(getCount).toBeGreaterThan(1));
    expect(screen.getByText("WORKING")).toBeInTheDocument();
    expect(screen.queryByText("AWAITING_CONFIRMATION")).not.toBeInTheDocument();
  });

  it("retains the monotonic anchor across an unavailable polling window", async () => {
    const responses: Array<Response> = [
      {
        ok: true,
        status: 200,
        json: async () => ({
          ...TICKET,
          order: {
            ...TICKET.order,
            state: "WORKING",
            state_version: 4,
            broker_order_id: "paper-1",
            updated_at_utc: "2099-07-21T14:30:04Z",
          },
        }),
      } as Response,
      { ok: false, status: 503, json: async () => ({}) } as Response,
      { ok: true, status: 200, json: async () => TICKET } as Response,
    ];
    vi.stubGlobal("fetch", vi.fn(async () => responses.shift() ?? responses[2]) as typeof fetch);
    render(<ExecutionPanel sessionId="live" canTrade />);
    expect(await screen.findByText("WORKING")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Refresh execution state" }));
    expect(await screen.findByText("Execution audit unavailable")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Refresh execution state" }));

    expect(await screen.findByText("WORKING")).toBeInTheDocument();
    expect(screen.queryByText("AWAITING_CONFIRMATION")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Confirm" })).toBeDisabled();
  });
});
