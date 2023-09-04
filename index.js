"use strict";

import MBTiles from "@mapbox/mbtiles";
import express from "express";
import fs from "node:fs";
import { parseArgs, promisify } from "node:util";
import TarStream from "tar-stream";
import zlib from "zlib";

const argsConfig = {
  port: {
    type: "string",
    short: "p",
    default: "3000",
  },
  mbtiles: {
    type: "string",
    short: "m",
  },
  config: {
    type: "string",
    short: "c",
  },
  "min-zoom": {
    type: "string",
    short: "z",
    default: "10",
  },
};

const { values: args } = parseArgs({
  options: argsConfig,
});

const app = express();

app.disable("x-powered-by");

app.get("/", (req, res) => {
  res.send("Bulk tile server is running");
});

app.get("/health", (req, res) => {
  res.send({ status: "OK" });
});

const sources = {};

if (args.mbtiles) {
  const source = await new (promisify(MBTiles))(args.mbtiles);
  const source_info = await promisify(source.getInfo.bind(source))();
  sources["default"] = { source, source_info };
}

if (args.config) {
  const config = JSON.parse(await fs.promises.readFile(args.config, "utf-8"));
  for (const [id, cfg] of Object.entries(config.sources)) {
    const source = await new (promisify(MBTiles))(cfg.path);
    const source_info = await promisify(source.getInfo.bind(source))();
    sources[id] = { source, source_info };
  }
}

const sizeCache = {};

app.get("/:id/:z(\\d+)/:x(\\d+)/:y(\\d+).:ext([\\w\\.]+)", async (req, res) => {
  const { id, ext } = req.params;
  const tileZ = parseInt(req.params.z);
  const tileX = parseInt(req.params.x);
  const tileY = parseInt(req.params.y);

  if (!(id in sources)) {
    res.status(404);
    res.json({
      error: `source '${id}' not found`,
    });
    return;
  }

  if (tileZ < parseInt(args["min-zoom"])) {
    res.status(400);
    res.json({
      error: `min zoom level is ${args["min-zoom"]}`,
    });
    return;
  }

  const { source, source_info } = sources[id];

  let stream;
  switch (ext) {
    case "tar":
      res.set("Content-Type", "application/x-tar");
      break;
    case "tar.gz":
      res.set("Content-Type", "application/x-tar+gzip");
      stream = zlib.createGzip();
      break;
    case "tar.br":
      res.set("Content-Type", "application/x-tar+brotli");
      stream = zlib.createBrotliCompress();
      break;
    default:
      res.status(404);
      res.json({
        error: `extension '${ext}' not supported`,
      });
      return;
  }

  const cacheKey = `${id}/${tileZ}/${tileX}/${tileY}`;

  /* For HEAD requests, return a cached Content-Length if available
     and skip computing a result that will be discarded */
  if (req.method === "HEAD") {
    if (cacheKey in sizeCache) {
      res.set("Content-Length", sizeCache[cacheKey]);
      res.end();
      return;
    }
  }

  const tarStream = TarStream.pack();
  if (stream) tarStream.pipe(stream);
  else stream = tarStream;

  const bufs = [];
  stream.on("data", (data) => {
    bufs.push(data);
  });
  stream.on("end", () => {
    const buf = Buffer.concat(bufs);
    res.send(buf);
    sizeCache[cacheKey] = buf.length;
  });

  for (let z = tileZ; z <= source_info.maxzoom; z++) {
    const tileCount = Math.pow(2, z - tileZ);
    for (let x = tileX * tileCount; x < (tileX + 1) * tileCount; x++) {
      for (let y = tileY * tileCount; y < (tileY + 1) * tileCount; y++) {
        try {
          const data = await promisify(source.getTile.bind(source))(z, x, y);
          const entry = tarStream.entry({
            name: `${z}/${x}/${y}`,
            size: data.length,
          });
          entry.write(data);
          entry.end();
        } catch {
          continue;
        }
      }
    }
  }

  tarStream.finalize();
});

const port = parseInt(args.port);

app.listen(port, () => {
  console.log(`Server is listening on port ${port}`);
});
