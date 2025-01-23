import './App.css'
import { createSignal, onMount } from 'solid-js'

export type InitialData = {
  count?: number;
};

type Clicked = {
  count: number
};

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
    const client = new trailbase.Client("http://localhost:4000");
    const api = client.records('counter');
    const stream = await api.subscribe(1);

    for await (const event of stream) {
      const update = event["Update"];
      if (update && update["value"] > count()) {
        setCount(update["value"]);
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
