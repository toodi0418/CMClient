export type WindowControlAction = "close" | "minimize" | "hide";

export interface WindowControlTarget {
  close(): Promise<void>;
  minimize(): Promise<void>;
  hide(): Promise<void>;
}

export async function runWindowControl(
  window: WindowControlTarget,
  action: WindowControlAction,
): Promise<void> {
  switch (action) {
    case "close":
      await window.close();
      return;
    case "minimize":
      await window.minimize();
      return;
    case "hide":
      await window.hide();
      return;
  }
}
