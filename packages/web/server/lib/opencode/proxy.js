import { createProxyMiddleware } from 'http-proxy-middleware';
import { createRequire } from 'module';

import {
  applyForwardProxyResponseHeaders,
  collectForwardProxyHeaders,
  shouldForwardProxyResponseHeader,
} from '../../proxy-headers.js';
import { createRealpathCache } from '../path-realpath-cache.js';

const require = createRequire(import.meta.url);

const openOpenCodeDb = async (dbPath) => {
  try {
    const { Database } = await import('bun:sqlite');
    const db = new Database(dbPath);
    return {
      run: (sql, params = []) => db.run(sql, params),
      close: () => db.close(),
    };
  } catch {
    const Database = require('better-sqlite3');
    const db = new Database(dbPath);
    return {
      run: (sql, params = []) => db.prepare(sql).run(...params),
      close: () => db.close(),
    };
  }
};

const isSessionUnarchivePatch = (body) => {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return false;
  }
  const bodyKeys = Object.keys(body);
  const timeKeys = body.time && typeof body.time === 'object' && !Array.isArray(body.time)
    ? Object.keys(body.time)
    : [];
  return Boolean(
    bodyKeys.length === 1
    && bodyKeys[0] === 'time'
    && timeKeys.length === 1
    && timeKeys[0] === 'archived'
    && body.time.archived === null
  );
};

export const createDirectoryQueryCanonicalizer = ({ realpath, ...cacheOptions } = {}) => {
  const realpathCache = createRealpathCache({ fallbackOnError: true, realpath, ...cacheOptions });

  return async (requestUrl) => {
    if (typeof requestUrl !== 'string' || !requestUrl.includes('directory=')) {
      return requestUrl;
    }

    const url = new URL(requestUrl, 'http://localhost');
    const directory = url.searchParams.get('directory');
    if (!directory) {
      return requestUrl;
    }

    const canonicalDirectory = await realpathCache.resolve(directory);
    if (!canonicalDirectory || canonicalDirectory === directory) {
      return requestUrl;
    }

    url.searchParams.set('directory', canonicalDirectory);
    return `${url.pathname}${url.search}`;
  };
};

export const waitForSseDrain = (res, signal) => new Promise((resolve) => {
  if (signal?.aborted || res.writableEnded || res.destroyed) {
    resolve();
    return;
  }

  const cleanup = () => {
    res.off?.('drain', onDone);
    res.off?.('close', onDone);
    res.off?.('error', onDone);
    signal?.removeEventListener?.('abort', onDone);
  };
  const onDone = () => {
    cleanup();
    resolve();
  };

  res.once?.('drain', onDone);
  res.once?.('close', onDone);
  res.once?.('error', onDone);
  signal?.addEventListener?.('abort', onDone, { once: true });
});

export const writeSseChunkWithBackpressure = async (res, value, signal) => {
  if (!value || value.length === 0 || signal?.aborted || res.writableEnded || res.destroyed) {
    return false;
  }

  const flushed = res.write(value);
  if (flushed !== false) {
    return true;
  }

  await waitForSseDrain(res, signal);
  return !signal?.aborted && !res.writableEnded && !res.destroyed;
};

export const createSseBoundaryTracker = () => {
  const decoder = new TextDecoder();
  let tail = '';

  const normalize = (value) => value.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  return {
    observe(value) {
      const text = typeof value === 'string'
        ? value
        : decoder.decode(value, { stream: true });
      if (text.length > 0) {
        tail = `${tail}${normalize(text)}`;
        if (tail.length > 4096) {
          tail = tail.slice(-4096);
        }
      }
      return this.isAtBoundary();
    },
    isAtBoundary() {
      return tail.length === 0 || tail.endsWith('\n\n');
    },
  };
};

const SESSION_LIST_ALLOWED_FIELDS = [
  'id',
  'slug',
  'projectID',
  'workspaceID',
  'directory',
  'path',
  'parentID',
  'title',
  'agent',
  'model',
  'version',
  'time',
  'cost',
  'tokens',
  'share',
  'metadata',
  'project',
];

export const sanitizeSessionListItem = (session) => {
  if (!session || typeof session !== 'object' || Array.isArray(session)) {
    return session;
  }

  const sanitized = {};
  for (const key of SESSION_LIST_ALLOWED_FIELDS) {
    if (key in session) {
      sanitized[key] = session[key];
    }
  }

  const summary = session.summary;
  if (summary && typeof summary === 'object' && !Array.isArray(summary)) {
    const summaryWithoutDiffs = { ...summary };
    delete summaryWithoutDiffs.diffs;
    sanitized.summary = summaryWithoutDiffs;
  }

  const revert = session.revert;
  if (revert && typeof revert === 'object' && !Array.isArray(revert)) {
    const revertMarker = {};
    if (typeof revert.messageID === 'string') {
      revertMarker.messageID = revert.messageID;
    }
    if (typeof revert.partID === 'string') {
      revertMarker.partID = revert.partID;
    }
    if (Object.keys(revertMarker).length > 0) {
      sanitized.revert = revertMarker;
    }
  }

  return sanitized;
};

export const sanitizeSessionListPayload = (payload) => {
  if (!Array.isArray(payload)) {
    return payload;
  }
  return payload.map((session) => sanitizeSessionListItem(session));
};

const parseArchivedQuery = (value) => {
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === true || raw === 'true') return true;
  if (raw === false || raw === 'false') return false;
  return null;
};

const filterSessionListByArchivedQuery = (payload, archived) => {
  if (!Array.isArray(payload) || archived === null) {
    return payload;
  }
  return payload.filter((session) => Boolean(session?.time?.archived) === archived);
};

const toFiniteNumber = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const getSessionUpdatedCursor = (session) => {
  return toFiniteNumber(session?.time?.updated ?? session?.time_updated);
};

const readNextSessionListCursor = (headers, payload) => {
  const headerCursor = toFiniteNumber(headers.get?.('x-next-cursor'));
  if (headerCursor !== undefined) {
    return headerCursor;
  }
  const lastSession = Array.isArray(payload) ? payload[payload.length - 1] : undefined;
  return getSessionUpdatedCursor(lastSession);
};

const parseSessionListLimit = (value) => {
  const raw = Array.isArray(value) ? value[0] : value;
  const parsed = Number.parseInt(String(raw ?? ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 100;
};

const setSessionListCursor = (requestPath, cursor) => {
  const url = new URL(requestPath, 'http://localhost');
  url.searchParams.set('cursor', String(cursor));
  return `${url.pathname}${url.search}`;
};

export const registerOpenCodeProxy = (app, deps) => {
  const {
    fs,
    os,
    path,
    OPEN_CODE_READY_GRACE_MS,
    getRuntime,
    getOpenCodeAuthHeaders,
    buildOpenCodeUrl,
    ensureOpenCodeApiPrefix,
  } = deps;

  if (app.get('opencodeProxyConfigured')) {
    return;
  }

  const runtime = getRuntime();
  if (runtime.openCodePort) {
    console.log(`Setting up proxy to OpenCode on port ${runtime.openCodePort}`);
  } else {
    console.log('Setting up OpenCode API gate (OpenCode not started yet)');
  }
  app.set('opencodeProxyConfigured', true);

  const isAbortError = (error) => error?.name === 'AbortError';
  const FALLBACK_PROXY_TARGET = 'http://127.0.0.1:3902';
  const canonicalizeDirectoryQuery = createDirectoryQueryCanonicalizer({
    realpath: fs?.promises?.realpath?.bind(fs.promises),
  });

  const hasParsedBodyValue = (body) => {
    if (body === undefined || body === null) return false;
    if (Buffer.isBuffer(body)) return body.length > 0;
    if (typeof body === 'string') return body.length > 0;
    if (Array.isArray(body)) return body.length > 0;
    if (typeof body === 'object') return Object.keys(body).length > 0;
    return true;
  };

  const getContentType = (proxyReq, req) => {
    const value = proxyReq.getHeader?.('content-type') ?? req.headers?.['content-type'] ?? '';
    if (Array.isArray(value)) return value[0] || '';
    return String(value || '');
  };

  const serializeUrlEncodedBody = (body) => {
    if (!body || typeof body !== 'object' || Buffer.isBuffer(body)) {
      return String(body ?? '');
    }

    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(body)) {
      if (value === undefined || value === null) continue;
      if (Array.isArray(value)) {
        for (const entry of value) {
          if (entry !== undefined && entry !== null) params.append(key, String(entry));
        }
        continue;
      }
      params.append(key, String(value));
    }
    return params.toString();
  };

  const serializeParsedBody = (req, proxyReq) => {
    if (req.method === 'GET' || req.method === 'HEAD') return null;
    if (req.body === undefined || req.body === null) return null;
    const originalContentLength = Number.parseInt(req.headers?.['content-length'] || '0', 10) || 0;
    if (!hasParsedBodyValue(req.body) && originalContentLength <= 0) return null;

    const contentType = getContentType(proxyReq, req).toLowerCase();
    if (Buffer.isBuffer(req.body)) return req.body;
    if (contentType.includes('application/json')) return Buffer.from(JSON.stringify(req.body));
    if (contentType.includes('application/x-www-form-urlencoded')) return Buffer.from(serializeUrlEncodedBody(req.body));
    if (typeof req.body === 'string') return Buffer.from(req.body);
    return null;
  };

  const replayParsedBody = (proxyReq, req) => {
    const body = serializeParsedBody(req, proxyReq);
    if (!body) return;
    proxyReq.setHeader('content-length', String(body.length));
    proxyReq.write(body);
  };

  const normalizeProxyTarget = (candidate) => {
    if (typeof candidate !== 'string') {
      return null;
    }

    const trimmed = candidate.trim();
    if (!trimmed) {
      return null;
    }

    return trimmed.replace(/\/+$/, '');
  };

  // Keep generic proxy requests on the same upstream base URL that health checks
  // and direct fetch helpers use. This avoids split-brain state where /health
  // succeeds against an external host but /api/* still proxies to 127.0.0.1.
  const resolveProxyTarget = () => {
    try {
      const resolved = normalizeProxyTarget(buildOpenCodeUrl('/', ''));
      if (resolved) {
        return resolved;
      }
    } catch {
    }

    const runtimeState = getRuntime();
    const externalBase = normalizeProxyTarget(runtimeState.openCodeBaseUrl);
    if (externalBase) {
      return externalBase;
    }

    if (runtimeState.openCodePort) {
      return `http://localhost:${runtimeState.openCodePort}`;
    }

    return FALLBACK_PROXY_TARGET;
  };

  const forwardSseRequest = async (req, res) => {
    const abortController = new AbortController();
    const closeUpstream = () => abortController.abort();
    let upstream = null;
    let reader = null;
    let heartbeatTimer = null;
    let writeQueue = Promise.resolve(true);
    const sseBoundary = createSseBoundaryTracker();

    req.on('close', closeUpstream);

    try {
      const requestUrl = typeof req.originalUrl === 'string' && req.originalUrl.length > 0
        ? req.originalUrl
        : (typeof req.url === 'string' ? req.url : '');
      const upstreamPath = requestUrl.startsWith('/api') ? requestUrl.slice(4) || '/' : requestUrl;
      const headers = collectForwardProxyHeaders(req.headers, getOpenCodeAuthHeaders());
      headers.accept ??= 'text/event-stream';
      headers['cache-control'] ??= 'no-cache';

      upstream = await fetch(buildOpenCodeUrl(upstreamPath, ''), {
        method: 'GET',
        headers,
        signal: abortController.signal,
      });

      res.status(upstream.status);
      applyForwardProxyResponseHeaders(upstream.headers, res);

      const contentType = upstream.headers.get('content-type') || 'text/event-stream';
      const isEventStream = contentType.toLowerCase().includes('text/event-stream');

      if (!upstream.body) {
        res.end(await upstream.text().catch(() => ''));
        return;
      }

      if (!isEventStream) {
        res.end(await upstream.text());
        return;
      }

      res.setHeader('Content-Type', contentType);
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      if (typeof res.flushHeaders === 'function') {
        res.flushHeaders();
      }

      // Disable TCP Nagle's algorithm so small SSE chunks are sent immediately
      // instead of being buffered up to ~200ms by the TCP stack.
      if (res.socket && typeof res.socket.setNoDelay === 'function') {
        res.socket.setNoDelay(true);
      }

      const SSE_HEARTBEAT_INTERVAL_MS = 20_000;

      const scheduleHeartbeat = () => {
        heartbeatTimer = setTimeout(async () => {
          if (abortController.signal.aborted || res.writableEnded || res.destroyed) {
            return;
          }
          if (!sseBoundary.isAtBoundary()) {
            scheduleHeartbeat();
            return;
          }
          const canContinue = await enqueueSseWrite(':heartbeat\n\n');
          if (canContinue) {
            scheduleHeartbeat();
          }
        }, SSE_HEARTBEAT_INTERVAL_MS);
      };

      const enqueueSseWrite = (value) => {
        writeQueue = writeQueue
          .catch(() => false)
          .then((canContinue) => {
            if (!canContinue) {
              return false;
            }
            return writeSseChunkWithBackpressure(res, value, abortController.signal);
          });
        return writeQueue;
      };

      scheduleHeartbeat();

      reader = upstream.body.getReader();
      while (!abortController.signal.aborted) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        if (value && value.length > 0) {
          sseBoundary.observe(value);
          const canContinue = await enqueueSseWrite(value);
          if (!canContinue) {
            break;
          }
        }
      }

      res.end();
    } catch (error) {
      if (isAbortError(error)) {
        return;
      }
      console.error('[proxy] OpenCode SSE proxy error:', error?.message ?? error);
      if (!res.headersSent) {
        res.status(503).json({ error: 'OpenCode service unavailable' });
      } else {
        res.end();
      }
    } finally {
      if (heartbeatTimer) {
        clearTimeout(heartbeatTimer);
        heartbeatTimer = null;
      }
      req.off('close', closeUpstream);
      try {
        if (reader) {
          await reader.cancel();
          reader.releaseLock();
        } else if (upstream?.body && !upstream.body.locked) {
          await upstream.body.cancel();
        }
      } catch {
      }
    }
  };

  const forwardSanitizedSessionListRequest = async (req, res, next, logLabel) => {
    try {
      const requestUrl = typeof req.originalUrl === 'string' && req.originalUrl.length > 0
        ? req.originalUrl
        : (typeof req.url === 'string' ? req.url : '');
      const upstreamPathRaw = requestUrl.startsWith('/api') ? requestUrl.slice(4) || '/' : requestUrl;
      const upstreamPath = await canonicalizeDirectoryQuery(upstreamPathRaw);
      const archivedFilter = parseArchivedQuery(req.query?.archived);
      const fetchSessionListPage = (pagePath) => fetch(buildOpenCodeUrl(pagePath, ''), {
        method: 'GET',
        headers: {
          ...collectForwardProxyHeaders(req.headers, getOpenCodeAuthHeaders()),
          accept: 'application/json',
          'accept-encoding': 'identity',
        },
      });

      const upstream = await fetchSessionListPage(upstreamPath);

      res.status(upstream.status);
      applyForwardProxyResponseHeaders(upstream.headers, res);

      const contentType = upstream.headers.get('content-type') || 'application/json; charset=utf-8';
      const bodyText = await upstream.text();
      if (!contentType.toLowerCase().includes('application/json')) {
        res.setHeader('content-type', contentType);
        res.end(bodyText);
        return;
      }

      let payload;
      try {
        payload = JSON.parse(bodyText);
      } catch {
        res.setHeader('content-type', contentType);
        res.end(bodyText);
        return;
      }

      if (Array.isArray(payload) && archivedFilter !== null) {
        const limit = parseSessionListLimit(req.query?.limit);
        const filtered = [];
        let nextCursor;
        let nextUpstreamPath = upstreamPath;
        let pagePayload = payload;
        let pageHeaders = upstream.headers;
        let previousCursor;

        while (true) {
          for (const session of pagePayload) {
            if (Boolean(session?.time?.archived) !== archivedFilter) {
              continue;
            }
            filtered.push(session);
            if (filtered.length >= limit) {
              nextCursor = getSessionUpdatedCursor(session);
              break;
            }
          }

          if (filtered.length >= limit || pagePayload.length === 0 || pagePayload.length < limit) {
            break;
          }

          const pageCursor = readNextSessionListCursor(pageHeaders, pagePayload);
          if (pageCursor === undefined || pageCursor === previousCursor) {
            break;
          }

          previousCursor = pageCursor;
          nextUpstreamPath = setSessionListCursor(nextUpstreamPath, pageCursor);
          const nextUpstream = await fetchSessionListPage(nextUpstreamPath);
          const nextContentType = nextUpstream.headers.get('content-type') || 'application/json; charset=utf-8';
          if (!nextContentType.toLowerCase().includes('application/json')) {
            break;
          }

          const nextBodyText = await nextUpstream.text();
          try {
            pagePayload = JSON.parse(nextBodyText);
          } catch {
            break;
          }
          if (!Array.isArray(pagePayload)) {
            break;
          }
          pageHeaders = nextUpstream.headers;
        }

        if (nextCursor !== undefined) {
          res.setHeader('x-next-cursor', String(nextCursor));
        } else {
          res.removeHeader('x-next-cursor');
        }

        res.setHeader('content-type', contentType);
        res.json(sanitizeSessionListPayload(filtered));
        return;
      }

      res.setHeader('content-type', contentType);
      res.json(sanitizeSessionListPayload(filterSessionListByArchivedQuery(payload, archivedFilter)));
    } catch (error) {
      if (isAbortError(error)) {
        return;
      }
      console.error(`[proxy] OpenCode ${logLabel} proxy error:`, error?.message ?? error);
      if (!res.headersSent) {
        res.status(503).json({ error: 'OpenCode service unavailable' });
        return;
      }
      next(error);
    }
  };

  // Ensure API prefix is detected before proxying
  app.use('/api', (_req, _res, next) => {
    ensureOpenCodeApiPrefix();
    next();
  });

  // Readiness gate — return 503 while OpenCode is starting/restarting
  app.use('/api', (req, res, next) => {
    if (
      req.path.startsWith('/themes/custom') ||
      req.path.startsWith('/push') ||
      req.path.startsWith('/config/agents') ||
      req.path.startsWith('/config/opencode-resolution') ||
      req.path.startsWith('/config/settings') ||
      req.path.startsWith('/config/skills') ||
      req.path === '/config/reload' ||
      req.path === '/health'
    ) {
      return next();
    }

    const runtimeState = getRuntime();
    const waitElapsed = runtimeState.openCodeNotReadySince === 0 ? 0 : Date.now() - runtimeState.openCodeNotReadySince;
    const stillWaiting =
      (!runtimeState.isOpenCodeReady && (runtimeState.openCodeNotReadySince === 0 || waitElapsed < OPEN_CODE_READY_GRACE_MS)) ||
      runtimeState.isRestartingOpenCode ||
      !runtimeState.openCodePort;

    if (stillWaiting) {
      return res.status(503).json({
        error: 'OpenCode is restarting',
        restarting: true,
      });
    }

    next();
  });

  // Windows: session merge for cross-directory session listing
  if (process.platform === 'win32') {
    app.get('/api/session', async (req, res, next) => {
      const rawUrl = req.originalUrl || req.url || '';
      if (rawUrl.includes('directory=')) return next();

      try {
        const authHeaders = getOpenCodeAuthHeaders();
        const fetchOpts = {
          method: 'GET',
          headers: { Accept: 'application/json', ...authHeaders },
          signal: AbortSignal.timeout(10000),
        };
        const globalRes = await fetch(buildOpenCodeUrl('/session', ''), fetchOpts);
        const globalPayload = globalRes.ok ? await globalRes.json().catch(() => []) : [];
        const globalSessions = Array.isArray(globalPayload) ? globalPayload : [];

        const settingsPath = path.join(os.homedir(), '.config', 'openchamber', 'settings.json');
        let projectDirs = [];
        try {
          const settingsRaw = fs.readFileSync(settingsPath, 'utf8');
          const settings = JSON.parse(settingsRaw);
          projectDirs = (settings.projects || [])
            .map((project) => (typeof project?.path === 'string' ? project.path.trim() : ''))
            .filter(Boolean);
        } catch {
        }

        const seen = new Set(
          globalSessions
            .map((session) => (session && typeof session.id === 'string' ? session.id : null))
            .filter((id) => typeof id === 'string')
        );
        const extraSessions = [];
        for (const dir of projectDirs) {
          const candidates = Array.from(new Set([
            dir,
            dir.replace(/\\/g, '/'),
            dir.replace(/\//g, '\\'),
          ]));
          for (const candidateDir of candidates) {
            const encoded = encodeURIComponent(candidateDir);
            try {
              const dirRes = await fetch(buildOpenCodeUrl(`/session?directory=${encoded}`, ''), fetchOpts);
              if (dirRes.ok) {
                const dirPayload = await dirRes.json().catch(() => []);
                const dirSessions = Array.isArray(dirPayload) ? dirPayload : [];
                for (const session of dirSessions) {
                  const id = session && typeof session.id === 'string' ? session.id : null;
                  if (id && !seen.has(id)) {
                    seen.add(id);
                    extraSessions.push(session);
                  }
                }
              }
            } catch {
            }
          }
        }

        const merged = [...globalSessions, ...extraSessions];
        merged.sort((a, b) => {
          const aTime = a && typeof a.time_updated === 'number' ? a.time_updated : 0;
          const bTime = b && typeof b.time_updated === 'number' ? b.time_updated : 0;
          return bTime - aTime;
        });
        console.log(`[SessionMerge] ${globalSessions.length} global + ${extraSessions.length} extra = ${merged.length} total`);
        return res.json(sanitizeSessionListPayload(merged));
      } catch (error) {
        console.log(`[SessionMerge] Error: ${error.message}, falling through`);
        next();
      }
    });
  }

  app.get('/api/session', (req, res, next) => {
    return forwardSanitizedSessionListRequest(req, res, next, 'session.list');
  });

  app.get('/api/global/event', forwardSseRequest);
  app.get('/api/event', forwardSseRequest);

  app.get('/api/experimental/session', (req, res, next) => {
    return forwardSanitizedSessionListRequest(req, res, next, 'experimental.session');
  });

  app.patch('/api/session/:sessionID', async (req, res, next) => {
    if (!isSessionUnarchivePatch(req.body)) {
      return next();
    }

    const sessionID = typeof req.params?.sessionID === 'string' ? req.params.sessionID.trim() : '';
    if (!sessionID) {
      return res.status(400).json({ error: 'Session ID is required' });
    }

    try {
      const openCodeDataPath = typeof process.env.OPENCODE_DATA_DIR === 'string' && process.env.OPENCODE_DATA_DIR.trim().length > 0
        ? path.resolve(process.env.OPENCODE_DATA_DIR.trim())
        : path.join(os.homedir(), '.local', 'share', 'opencode');
      const dbPath = path.join(openCodeDataPath, 'opencode.db');
      if (!fs.existsSync(dbPath)) {
        return res.status(503).json({ error: 'OpenCode database is not available' });
      }

      const db = await openOpenCodeDb(dbPath);
      try {
        const result = db.run('UPDATE session SET time_archived = NULL, time_updated = ? WHERE id = ?', [Date.now(), sessionID]);
        if (result.changes === 0) {
          return res.status(404).json({ error: 'Session not found' });
        }
      } finally {
        db.close();
      }

      const query = new URLSearchParams();
      if (typeof req.query?.directory === 'string' && req.query.directory.length > 0) {
        query.set('directory', req.query.directory);
      }
      if (typeof req.query?.workspace === 'string' && req.query.workspace.length > 0) {
        query.set('workspace', req.query.workspace);
      }
      const suffix = query.toString() ? `?${query.toString()}` : '';
      const response = await fetch(buildOpenCodeUrl(`/session/${encodeURIComponent(sessionID)}${suffix}`, ''), {
        headers: {
          ...collectForwardProxyHeaders(req.headers, getOpenCodeAuthHeaders()),
          accept: 'application/json',
          'accept-encoding': 'identity',
        },
      });
      res.status(response.status);
      applyForwardProxyResponseHeaders(response.headers, res);
      return res.send(await response.text());
    } catch (error) {
      console.error('[proxy] Failed to unarchive OpenCode session:', error?.message ?? error);
      return res.status(500).json({ error: 'Failed to unarchive session' });
    }
  });

  // Generic proxy for non-SSE OpenCode API routes.
  const apiProxy = createProxyMiddleware({
    target: resolveProxyTarget(),
    changeOrigin: true,
    pathRewrite: { '^/api': '' },
    // Dynamic target — port can change after restart
    router: () => resolveProxyTarget(),
    on: {
      proxyReq: (proxyReq, req) => {
        // Inject OpenCode auth headers
        const authHeaders = getOpenCodeAuthHeaders();
        if (authHeaders.Authorization) {
          proxyReq.setHeader('Authorization', authHeaders.Authorization);
        }

        // Defensive: request identity encoding from upstream OpenCode.
        // This avoids compressed-body/header mismatches in multi-proxy setups.
        proxyReq.setHeader('accept-encoding', 'identity');

        replayParsedBody(proxyReq, req);
      },
      proxyRes: (proxyRes) => {
        for (const key of Object.keys(proxyRes.headers || {})) {
          if (!shouldForwardProxyResponseHeader(key)) {
            delete proxyRes.headers[key];
          }
        }
      },
      error: (err, _req, res) => {
        console.error('[proxy] OpenCode proxy error:', err.message);
        if (res && !res.headersSent && typeof res.status === 'function') {
          res.status(503).json({ error: 'OpenCode service unavailable' });
        }
      },
    },
  });

  // Best-effort fallback for stale clients still sending symlink paths.
  // Settings and project selection normalize at source; this cached async path
  // avoids blocking the proxy hot path on every directory-scoped request.
  app.use('/api', async (req, _res, next) => {
    try {
      const rewrittenUrl = await canonicalizeDirectoryQuery(req.url);
      if (rewrittenUrl !== req.url) {
        req.url = rewrittenUrl;
      }
    } catch {
      // Pass through as-is if URL parsing or realpath resolution fails.
    }
    next();
  });

  app.use('/api', apiProxy);
};
