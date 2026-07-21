import { describe, expect, it } from "vitest";
import { isNewerExecutionOrder, parseExecutionTicket } from "./execution";
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
  });
});
