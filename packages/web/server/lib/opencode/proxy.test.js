import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Database } from 'bun:sqlite';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';
import request from 'supertest';

import { createDirectoryQueryCanonicalizer, registerOpenCodeProxy } from './proxy.js';

describe('createDirectoryQueryCanonicalizer', () => {
  it('canonicalizes directory query params and preserves other params', async () => {
    const canonicalize = createDirectoryQueryCanonicalizer({
      realpath: async (value) => value === '/link/project' ? '/real/project' : value,
    });

    await expect(canonicalize('/session?foo=1&directory=/link/project&bar=2'))
      .resolves.toBe('/session?foo=1&directory=%2Freal%2Fproject&bar=2');
  });

  it('caches directory realpath lookups', async () => {
    let calls = 0;
    const canonicalize = createDirectoryQueryCanonicalizer({
      realpath: async () => {
        calls += 1;
        return '/real/project';
      },
    });

    await expect(canonicalize('/session?directory=/link/project')).resolves.toBe('/session?directory=%2Freal%2Fproject');
    await expect(canonicalize('/session?directory=/link/project')).resolves.toBe('/session?directory=%2Freal%2Fproject');
    expect(calls).toBe(1);
  });

  it('deduplicates concurrent directory realpath lookups', async () => {
    let calls = 0;
    let release = () => undefined;
    const pending = new Promise((resolve) => {
      release = () => resolve('/real/project');
    });
    const canonicalize = createDirectoryQueryCanonicalizer({
      realpath: async () => {
        calls += 1;
        return pending;
      },
    });

    const first = canonicalize('/session?directory=/link/project');
    const second = canonicalize('/session?directory=/link/project');
    await Promise.resolve();

    expect(calls).toBe(1);
    release();
    await expect(Promise.all([first, second])).resolves.toEqual([
      '/session?directory=%2Freal%2Fproject',
      '/session?directory=%2Freal%2Fproject',
    ]);
  });

  it('falls back to the original URL when realpath fails', async () => {
    const canonicalize = createDirectoryQueryCanonicalizer({
      realpath: async () => {
        throw new Error('missing');
      },
    });

    await expect(canonicalize('/session?foo=1&directory=/missing/project'))
      .resolves.toBe('/session?foo=1&directory=/missing/project');
  });

  it('leaves URLs without directory params unchanged', async () => {
    const canonicalize = createDirectoryQueryCanonicalizer({
      realpath: async () => '/real/project',
    });

    await expect(canonicalize('/session?foo=1')).resolves.toBe('/session?foo=1');
  });
});

const createProxyDependencies = (options = {}) => ({
  fs,
  os,
  path,
  OPEN_CODE_READY_GRACE_MS: 0,
  getRuntime: () => ({
    openCodePort: 1234,
    isOpenCodeReady: true,
    isRestartingOpenCode: false,
    openCodeNotReadySince: 0,
  }),
  getOpenCodeAuthHeaders: () => ({ authorization: 'Bearer test' }),
  buildOpenCodeUrl: (route) => `${options.baseUrl ?? 'http://opencode.test'}${route}`,
  ensureOpenCodeApiPrefix: vi.fn(),
});

describe('registerOpenCodeProxy session unarchive compatibility', () => {
  const originalFetch = globalThis.fetch;
  const originalOpencodeDataDir = process.env.OPENCODE_DATA_DIR;
  let dataDir;

  beforeEach(() => {
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'openchamber-opencode-proxy-test-'));
    process.env.OPENCODE_DATA_DIR = dataDir;

    const db = new Database(path.join(dataDir, 'opencode.db'));
    try {
      db.exec('CREATE TABLE session (id text PRIMARY KEY, time_archived integer, time_updated integer NOT NULL)');
      db.run('INSERT INTO session (id, time_archived, time_updated) VALUES (?, ?, ?)', ['ses_1', 123, 1]);
    } finally {
      db.close();
    }
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    if (typeof originalOpencodeDataDir === 'string') {
      process.env.OPENCODE_DATA_DIR = originalOpencodeDataDir;
    } else {
      delete process.env.OPENCODE_DATA_DIR;
    }
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  it('clears session time_archived for archived null patches before generic proxying', async () => {
    const app = express();
    app.use(express.json());
    registerOpenCodeProxy(app, createProxyDependencies());

    globalThis.fetch = vi.fn(async () => new Response(JSON.stringify({
      id: 'ses_1',
      title: 'Restored session',
      time: { created: 1, updated: 2 },
    }), { headers: { 'content-type': 'application/json' } }));

    await request(app)
      .patch('/api/session/ses_1?directory=%2Frepo')
      .send({ time: { archived: null } })
      .expect(200, {
        id: 'ses_1',
        title: 'Restored session',
        time: { created: 1, updated: 2 },
      });

    const db = new Database(path.join(dataDir, 'opencode.db'), { readonly: true });
    try {
      const row = db.query('SELECT time_archived FROM session WHERE id = ?').get('ses_1');
      expect(row.time_archived).toBeNull();
    } finally {
      db.close();
    }

    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
    expect(globalThis.fetch.mock.calls[0][0]).toBe('http://opencode.test/session/ses_1?directory=%2Frepo');
    expect(globalThis.fetch.mock.calls[0][1].headers).toEqual(expect.objectContaining({
      accept: 'application/json',
      'accept-encoding': 'identity',
    }));
  });

  it('lets mixed session patches fall through to the generic proxy', async () => {
    const app = express();
    app.use(express.json());
    registerOpenCodeProxy(app, createProxyDependencies({ baseUrl: 'http://127.0.0.1:1' }));

    await request(app)
      .patch('/api/session/ses_1')
      .send({ title: 'New title', time: { archived: null } })
      .expect(503, { error: 'OpenCode service unavailable' });

    const db = new Database(path.join(dataDir, 'opencode.db'), { readonly: true });
    try {
      const row = db.query('SELECT time_archived FROM session WHERE id = ?').get('ses_1');
      expect(row.time_archived).toBe(123);
    } finally {
      db.close();
    }
  });
});

describe('registerOpenCodeProxy session list filtering', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('filters experimental session lists by actual archived timestamp', async () => {
    const app = express();
    registerOpenCodeProxy(app, createProxyDependencies());

    globalThis.fetch = vi.fn(async () => new Response(JSON.stringify([
      { id: 'ses_active', title: 'Active', time: { created: 1, updated: 3 } },
      { id: 'ses_archived', title: 'Archived', time: { created: 1, updated: 2, archived: 123 } },
    ]), { headers: { 'content-type': 'application/json' } }));

    await request(app)
      .get('/api/experimental/session?archived=true&limit=100')
      .expect(200, [
        { id: 'ses_archived', title: 'Archived', time: { created: 1, updated: 2, archived: 123 } },
      ]);

    await request(app)
      .get('/api/experimental/session?archived=false&limit=100')
      .expect(200, [
        { id: 'ses_active', title: 'Active', time: { created: 1, updated: 3 } },
      ]);
  });

  it('continues upstream pagination when a filtered page has no matches', async () => {
    const app = express();
    registerOpenCodeProxy(app, createProxyDependencies());

    globalThis.fetch = vi.fn(async (url) => {
      const cursor = new URL(String(url)).searchParams.get('cursor');
      if (!cursor) {
        return new Response(JSON.stringify([
          { id: 'ses_active_1', title: 'Active 1', time: { created: 1, updated: 30 } },
          { id: 'ses_active_2', title: 'Active 2', time: { created: 1, updated: 20 } },
        ]), { headers: { 'content-type': 'application/json', 'x-next-cursor': '20' } });
      }

      return new Response(JSON.stringify([
        { id: 'ses_archived', title: 'Archived', time: { created: 1, updated: 10, archived: 123 } },
      ]), { headers: { 'content-type': 'application/json' } });
    });

    await request(app)
      .get('/api/experimental/session?archived=true&limit=2')
      .expect(200, [
        { id: 'ses_archived', title: 'Archived', time: { created: 1, updated: 10, archived: 123 } },
      ]);

    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
    expect(globalThis.fetch.mock.calls[1][0]).toBe('http://opencode.test/experimental/session?archived=true&limit=2&cursor=20');
  });
});
