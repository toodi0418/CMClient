import type { SetupPhase } from "@cmclient/contracts";
import { assign, setup } from "xstate";

export type SetupFlowEvent =
  | { type: "AGENT_PHASE_CHANGED"; phase: SetupPhase }
  | { type: "REVIEW" }
  | { type: "EDIT" };

interface SetupFlowContext {
  agentPhase: SetupPhase;
}

const flow = setup({
  types: {
    context: {} as SetupFlowContext,
    events: {} as SetupFlowEvent,
  },
  actions: {
    projectAgentPhase: assign({
      agentPhase: ({ context, event }) =>
        event.type === "AGENT_PHASE_CHANGED" ? event.phase : context.agentPhase,
    }),
  },
  guards: {
    isTerms: ({ event }) =>
      event.type === "AGENT_PHASE_CHANGED" &&
      (event.phase === "uninitialized" || event.phase === "terms_required"),
    isSameCredentialsPhase: ({ context, event }) =>
      event.type === "AGENT_PHASE_CHANGED" &&
      event.phase === "credentials_required" &&
      context.agentPhase === "credentials_required",
    isCredentials: ({ event }) =>
      event.type === "AGENT_PHASE_CHANGED" &&
      event.phase === "credentials_required",
    isValidating: ({ event }) =>
      event.type === "AGENT_PHASE_CHANGED" && event.phase === "validating",
    isReady: ({ event }) =>
      event.type === "AGENT_PHASE_CHANGED" && event.phase === "ready",
    isRecovery: ({ event }) =>
      event.type === "AGENT_PHASE_CHANGED" &&
      event.phase === "recovery_required",
  },
}).createMachine({
  id: "setupFlow",
  initial: "synchronizing",
  context: { agentPhase: "uninitialized" },
  on: {
    AGENT_PHASE_CHANGED: [
      {
        guard: "isTerms",
        target: ".terms",
        actions: "projectAgentPhase",
      },
      {
        guard: "isSameCredentialsPhase",
        actions: "projectAgentPhase",
      },
      {
        guard: "isCredentials",
        target: ".credentials.connection",
        actions: "projectAgentPhase",
      },
      {
        guard: "isValidating",
        target: ".validating",
        actions: "projectAgentPhase",
      },
      {
        guard: "isReady",
        target: ".finish",
        actions: "projectAgentPhase",
      },
      {
        guard: "isRecovery",
        target: ".recovery",
        actions: "projectAgentPhase",
      },
    ],
  },
  states: {
    synchronizing: {},
    terms: {},
    credentials: {
      initial: "connection",
      states: {
        connection: {
          on: { REVIEW: "review" },
        },
        review: {
          on: { EDIT: "connection" },
        },
      },
    },
    validating: {},
    finish: {},
    recovery: {},
  },
});

/**
 * XState owns only transient view navigation. Agent status events can always
 * replace it, so the browser never becomes a second setup authority.
 */
export const setupFlowMachine = flow;
