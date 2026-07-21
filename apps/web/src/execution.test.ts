import { describe, expect, it } from "vitest";
import { classifyExecutionProjection, isNewerExecutionOrder, parseExecutionTicket } from "./execution";
import { EXECUTION_TICKET_FIXTURE as TICKET } from "./executionTestFixture";

describe("parseExecutionTicket", () => {
  it("accepts a matching strict plan/order pair", () => {
    expect(parseExecutionTicket(TICKET)?.order.state).toBe("AWAITING_CONFIRMATION");
  });

  it("rejects identity mismatch, overfill and unknown state", () => {
    expect(
      parseExecutionTicket({ ...TICKET, order: { ...TICKET.order, plan_hash: "b".repeat(64) } }),
    ).toBeNull();
    expect(
      parseExecutionTicket({ ...TICKET, order: { ...TICKET.order, filled_quantity: 2 } }),
    ).toBeNull();
    expect(parseExecutionTicket({ ...TICKET, order: { ...TICKET.order, state: "MAYBE" } })).toBeNull();
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: {
          ...TICKET.plan,
          legs: [
            ...TICKET.plan.legs,
            { ...TICKET.plan.legs[0], contract_id: "QQQ-20990721-P-500", quantity: 2 },
          ],
        },
      }),
    ).toBeNull();
    expect(
      parseExecutionTicket({ ...TICKET, order: { ...TICKET.order, total_quantity: 2 } }),
    ).toBeNull();
  });

  it("rejects malformed decimal and non-Z timestamps", () => {
    expect(
      parseExecutionTicket({ ...TICKET, plan: { ...TICKET.plan, max_loss: "NaN" } }),
    ).toBeNull();
    expect(
      parseExecutionTicket({ ...TICKET, order: { ...TICKET.order, updated_at_utc: "2099-07-21" } }),
    ).toBeNull();
  });

  it("rejects legacy or incomplete quote and adaptive contracts", () => {
    expect(
      parseExecutionTicket({ ...TICKET, plan: { ...TICKET.plan, schema_version: "1.0" } }),
    ).toBeNull();
    const { quote: _quote, ...legWithoutQuote } = TICKET.plan.legs[0];
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: { ...TICKET.plan, legs: [legWithoutQuote] },
      }),
    ).toBeNull();
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: {
          ...TICKET.plan,
          legs: [{ ...TICKET.plan.legs[0], quote: { ...TICKET.plan.legs[0].quote, provider: "BROKER" } }],
        },
      }),
    ).toBeNull();
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: { ...TICKET.plan, market_data_provider: "BROKER" },
      }),
    ).toBeNull();
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: { ...TICKET.plan, order_type: "ADAPTIVE_LIMIT" },
      }),
    ).toBeNull();
  });

  it("accepts only zero-risk single-leg market closes", () => {
    const close = {
      ...TICKET,
      plan: {
        ...TICKET.plan,
        position_effect: "CLOSE",
        order_side: "SELL",
        order_type: "MARKET",
        max_loss: "0.00",
        legs: [{ ...TICKET.plan.legs[0], side: "SELL" }],
      },
    };
    expect(parseExecutionTicket(close)?.plan.position_effect).toBe("CLOSE");
    expect(
      parseExecutionTicket({ ...close, plan: { ...close.plan, max_loss: "1.00" } }),
    ).toBeNull();
    expect(
      parseExecutionTicket({
        ...TICKET,
        plan: { ...TICKET.plan, order_type: "MARKET" },
      }),
    ).toBeNull();
  });

  it("rejects stale or conflicting order projections", () => {
    const current = { ...TICKET.order, state: "WORKING" as const, state_version: 4, filled_quantity: 1 };
    expect(isNewerExecutionOrder(current, { ...TICKET.order, state_version: 1 })).toBe(false);
    expect(
      isNewerExecutionOrder(current, {
        ...current,
        state: "AWAITING_CONFIRMATION",
        updated_at_utc: "2099-07-21T14:30:02Z",
      }),
    ).toBe(false);
    expect(
      isNewerExecutionOrder(current, {
        ...current,
        state: "PARTIAL_FILL",
        state_version: 5,
        updated_at_utc: "2099-07-21T14:30:02Z",
      }),
    ).toBe(true);
    expect(
      isNewerExecutionOrder(current, {
        ...current,
        state: "PARTIAL_FILL",
        state_version: 5,
        filled_quantity: 0,
        updated_at_utc: "2099-07-21T14:30:02Z",
      }),
    ).toBe(false);
    expect(
      isNewerExecutionOrder(current, {
        ...current,
        residual_exposure: true,
        broker_child_order_ids: ["child-1"],
        updated_at_utc: "2099-07-21T14:30:02Z",
      }),
    ).toBe(false);
  });

  it("derives residual exposure from full child projections", () => {
    const child = {
      broker_order_id: "child-1",
      leg_index: 0,
      contract_id: TICKET.plan.legs[0].contract_id,
      side: "BUY" as const,
      quantity: 1,
      filled_quantity: 0,
      state: "WORKING" as const,
      submitted_price: "1.25",
    };
    const invalid = {
      ...TICKET,
      order: {
        ...TICKET.order,
        state: "WORKING",
        broker_child_order_ids: ["child-1"],
        broker_child_orders: [child],
        residual_exposure: false,
      },
    };
    expect(parseExecutionTicket(invalid)).toBeNull();
    expect(
      parseExecutionTicket({
        ...invalid,
        order: { ...invalid.order, residual_exposure: true },
      })?.order.broker_child_orders[0]?.filled_quantity,
    ).toBe(0);
  });

  it("classifies same-version action conflicts for reconciliation", () => {
    const current = { ...TICKET.order, state: "WORKING" as const, state_version: 4 };
    expect(classifyExecutionProjection(current, { ...current })).toBe("DUPLICATE");
    expect(
      classifyExecutionProjection(current, { ...current, state: "FILLED" }),
    ).toBe("CONFLICT");
    expect(
      classifyExecutionProjection(current, { ...current, state_version: 3 }),
    ).toBe("STALE");
  });
});
