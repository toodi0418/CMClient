import { GatewayApiClient } from "@cmclient/api-client";

import { createManagementRequestQueue } from "./management-request-queue";

export const managementRequestQueue = createManagementRequestQueue();
export const managementApi = new GatewayApiClient({
  fetch: managementRequestQueue.fetch,
});
