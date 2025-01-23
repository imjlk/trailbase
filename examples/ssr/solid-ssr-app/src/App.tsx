import './App.css'
import { createSignal, onMount } from 'solid-js'

export type Clicked = {
  count: number
};

declare global {
  interface Window {
    __INITIAL_DATA__: Clicked | null;
  }
}

export function App({initialCount }: {initialCount?: number}) {
  const [count, setCount] = createSignal(initialCount ?? 0)

  const onClick = () => {
    setCount((count) => count + 1);

    fetch('/clicked').then(async (response) => {
      const clicked = (await response.json()) as Clicked;
      if (clicked.count > count()) {
        setCount(clicked.count);
      }
    });
  };

  onMount(async () => {
    const trailbase = await import("trailbase");

    const client = new trailbase.Client(window.location.origin);
    const api = client.records('counter');
    const reader = (await api.subscribe(1)).getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done || !value) {
        break;
      }

      const update = value as { Update?: { value? : number}};
      const updatedCount = update.Update?.value;
      if (updatedCount && updatedCount > count()) {
        setCount(updatedCount);
      }
    }
  });

  return (
    <div class="App">
      <h1>TrailBase Cookie Clicker</h1>

      <div class="card">
        <button onClick={onClick}>
          count is {count()}
        </button>

        <p>
          Click the counter
        </p>
      </div>
    </div>
  )
}
