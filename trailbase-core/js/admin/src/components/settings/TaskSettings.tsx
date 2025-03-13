import { createResource, For, Switch, Match } from "solid-js";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

import { listTasks } from "@/lib/tasks";
import type { Task } from "@bindings/Task";

export function TaskSettings(_props: {
  markDirty: () => void;
  postSubmit: () => void;
}) {
  const [taskList] = createResource(listTasks);

  return (
    <Switch>
      <Match when={taskList.error}>info.error</Match>
      <Match when={taskList.loading}>Loading...</Match>
      <Match when={taskList()}>
        <Table>
          <TableHeader>
            <TableHead>Id</TableHead>
            <TableHead>Name</TableHead>
            <TableHead>Schedule</TableHead>
            <TableHead>Action</TableHead>
          </TableHeader>

          <TableBody>
            <For each={taskList()?.tasks}>
              {(task: Task) => (
                <TableRow>
                  <TableCell>{task.id}</TableCell>

                  <TableCell>{task.name}</TableCell>

                  <TableCell>{task.schedule}</TableCell>

                  <TableCell>Actions</TableCell>
                </TableRow>
              )}
            </For>
          </TableBody>
        </Table>
      </Match>
    </Switch>
  );
}
