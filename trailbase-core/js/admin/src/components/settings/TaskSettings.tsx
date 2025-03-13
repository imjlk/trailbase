import { createResource, For, Switch, Match } from "solid-js";
import { createForm } from "@tanstack/solid-form";
import { TbPlayerPlay, TbHistory } from "solid-icons/tb";

import { Button } from "@/components/ui/button";
import { IconButton } from "@/components/IconButton";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TextField, TextFieldInput } from "@/components/ui/text-field";

import { type FieldApiT, notEmptyValidator } from "@/components/FormFields";
import { Config, CronConfig, SystemCronJob } from "@proto/config";
import { createConfigQuery, setConfig } from "@/lib/config";
import { listTasks } from "@/lib/tasks";
import type { Task } from "@bindings/Task";

type CronJobProxy = {
  /// Set to false if the loaded config contained the job.
  default: boolean;
  initial: SystemCronJob;
  config: SystemCronJob;
  task?: Task;
};

type FormProxy = {
  jobs: CronJobProxy[];
};

function equal(a: SystemCronJob, b: SystemCronJob): boolean {
  return a.disableJob === b.disableJob && a.spec === b.spec && a.id === b.id;
}

function buildFormProxy(
  config: CronConfig | undefined,
  tasks: Task[],
): FormProxy {
  const result = new Map<number, CronJobProxy>();
  if (config) {
    for (const job of config.systemJobs) {
      const id = job.id;
      if (id) {
        result.set(id, {
          default: false,
          initial: job,
          config: { ...job },
        });
      }
    }
  }

  for (const task of tasks) {
    const d: SystemCronJob = {
      id: task.id,
      spec: task.schedule,
    };
    const entry: CronJobProxy = result.get(task.id) ?? {
      default: true,
      initial: d,
      config: { ...d },
    };
    entry.task = task;
    result.set(task.id, entry);
  }

  return {
    jobs: [...result.values()],
  };
}

function extractConfig(proxy: FormProxy): CronConfig {
  const systemJobs: SystemCronJob[] = [];

  for (const entry of proxy.jobs) {
    // Only add entries that were part of the original config or have changed from the initial default.
    if (entry.default === false) {
      systemJobs.push(entry.config);
    } else if (!equal(entry.initial, entry.config)) {
      systemJobs.push(entry.config);
    }
  }

  return {
    systemJobs,
  };
}

export function TaskSettingsImpl(props: {
  markDirty: () => void;
  postSubmit: () => void;
  config: Config;
  tasks: Task[];
  refetchTasks: () => void;
}) {
  const form = createForm(() => ({
    defaultValues: buildFormProxy(props.config.cron, props.tasks),
    onSubmit: async ({ value }: { value: FormProxy }) => {
      const newConfig = {
        ...props.config,
        cron: extractConfig(value),
      };

      await setConfig(newConfig);
      props.postSubmit();
    },
  }));

  form.useStore((state) => {
    if (state.isDirty && !state.isSubmitted) {
      props.markDirty();
    }
  });

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        e.stopPropagation();
        form.handleSubmit();
      }}
    >
      <Table>
        <TableHeader>
          <TableHead>Id</TableHead>
          <TableHead>Name</TableHead>
          <TableHead>Schedule</TableHead>
          <TableHead>Enabled</TableHead>
          <TableHead>Action</TableHead>
        </TableHeader>

        <TableBody>
          <For each={props.tasks}>
            {(task: Task, index: () => number) => (
              <TableRow>
                <TableCell>{task.id}</TableCell>

                <TableCell>{task.name}</TableCell>

                <TableCell>
                  <form.Field
                    name={`jobs[${index()}].config.spec`}
                    validators={notEmptyValidator()}
                  >
                    {(field: () => FieldApiT<string | undefined>) => {
                      console.log(field().state);

                      return (
                        <TextField>
                          <TextFieldInput
                            type="text"
                            value={field().state.value}
                            onBlur={field().handleBlur}
                            autocomplete="off"
                            onKeyUp={(e: Event) => {
                              field().handleChange(
                                (e.target as HTMLInputElement).value,
                              );
                            }}
                          />
                        </TextField>
                      );
                    }}
                  </form.Field>
                </TableCell>

                <TableCell>
                  <Checkbox />
                </TableCell>

                <TableCell>
                  <div class="flex h-full items-center">
                    <IconButton
                      onClick={() => {
                        props.refetchTasks();
                      }}
                    >
                      <TbPlayerPlay size={20} />
                    </IconButton>

                    <IconButton onClick={() => {}}>
                      <TbHistory size={20} />
                    </IconButton>
                  </div>
                </TableCell>
              </TableRow>
            )}
          </For>
        </TableBody>
      </Table>

      <div class="flex justify-end pt-4">
        <form.Subscribe
          selector={(state) => ({
            canSubmit: state.canSubmit,
            isSubmitting: state.isSubmitting,
          })}
        >
          {(state) => {
            return (
              <Button
                type="submit"
                disabled={!state().canSubmit}
                variant="default"
              >
                {state().isSubmitting ? "..." : "Submit"}
              </Button>
            );
          }}
        </form.Subscribe>
      </div>
    </form>
  );
}

export function TaskSettings(props: {
  markDirty: () => void;
  postSubmit: () => void;
}) {
  const config = createConfigQuery();
  const [taskList, { refetch }] = createResource(listTasks);

  return (
    <Switch fallback="Loading...">
      <Match when={taskList.error}>{taskList.error}</Match>
      <Match when={config.error}>{JSON.stringify(config.error)}</Match>

      <Match when={taskList() && config.data?.config}>
        <TaskSettingsImpl
          {...props}
          config={config.data!.config!}
          tasks={taskList()?.tasks ?? []}
          refetchTasks={refetch}
        />
      </Match>
    </Switch>
  );
}
