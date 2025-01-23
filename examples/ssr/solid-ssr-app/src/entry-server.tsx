import { renderToString, generateHydrationScript } from 'solid-js/web'
import { App, type InitialData } from './App'

export function render(_url: string, count: number) {
  const head = generateHydrationScript();
  const html = renderToString(() => <App initialCount={count} />);

  const initialData : InitialData = {
    count,
  };
  const data = `<script>window.__INITIAL_DATA__ = ${JSON.stringify(initialData)};</script>`;

  return {
    head,
    html,
    data,
  };
}
