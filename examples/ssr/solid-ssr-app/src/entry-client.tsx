/* @refresh reload */
import './index.css'
import { hydrate } from 'solid-js/web'
import { App, type InitialData } from './App'

hydrate(() => {
  const initialData = window.__INITIAL_DATA__ as InitialData;
  return (
    <App initialCount={initialData.count ?? 0} />
  );
}, document.getElementById('root') as HTMLElement,
)
