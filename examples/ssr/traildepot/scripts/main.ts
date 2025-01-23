import { addRoute, htmlHandler } from "../trailbase.js";
import { render } from "./entry-server.js";

let _template : Promise<string> | null = null;

async function getTemplate() : Promise<string> {
  if (_template == null) {
    const template = _template = Deno.readTextFile('solid-ssr-app/dist/client/index.html');
    return await template;
  }
  return await _template;
}

/// Register a root handler.
addRoute(
  "GET",
  "/",
  htmlHandler(async (req) => {
    // NOTE: this is replicating solid-ssr-app/server.js;
    const url = req.uri;
    const rendered = render(url);

    const html = (await getTemplate())
      .replace(`<!--app-head-->`, rendered.head ?? '')
      .replace(`<!--app-html-->`, rendered.html ?? '')
    return html;
  }),
);
