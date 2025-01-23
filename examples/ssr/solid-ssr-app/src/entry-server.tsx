import { renderToString, generateHydrationScript } from 'solid-js/web'
import App from './App'

export function render(_url: string) {
  const head = generateHydrationScript();
  const html = renderToString(() => <App />)

  return {
    head,
    html,
  };
}
