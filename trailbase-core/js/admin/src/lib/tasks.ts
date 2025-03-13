import { adminFetch } from "@/lib/fetch";

import type { ListTasksResponse } from "@bindings/ListTasksResponse";
import type { RunTaskRequest } from "@bindings/RunTaskRequest";
import type { RunTaskResponse } from "@bindings/RunTaskResponse";

export async function listTasks(): Promise<ListTasksResponse> {
  const response = await adminFetch("/tasks", {
    method: "GET",
  });
  return await response.json();
}

export async function runTasks(
  request: RunTaskRequest,
): Promise<RunTaskResponse> {
  const response = await adminFetch("/tasks", {
    method: "POST",
    body: JSON.stringify(request),
  });
  return await response.json();
}
