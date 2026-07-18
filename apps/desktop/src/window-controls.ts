export type WindowControlAction = "exit" | "minimize" | "hide";

export interface WindowControlTarget {
  exit(): Promise<void>;
  minimize(): Promise<void>;
  hide(): Promise<void>;
}

export async function runWindowControl(
  window: WindowControlTarget,
  action: WindowControlAction,
): Promise<void> {
  switch (action) {
    case "exit":
      await window.exit();
      return;
    case "minimize":
      await window.minimize();
      return;
    case "hide":
      await window.hide();
      return;
  }
}
