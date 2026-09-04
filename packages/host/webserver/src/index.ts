/**
 * @deepseek-ai/dsh-host-webserver — node:http route registration with optional
 * gzip, index injection, and one fallback seat. It knows no harness concepts
 * and serves no files; the composing application owns dist serving. Electron
 * uses file:// plus IPC instead, and this package never prints the URL.
 * Route handlers retain direct response ownership.
 */

import { createServer } from 'node:http'
import type { IncomingMessage, ServerResponse, Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import type { Duplex } from 'node:stream'
import { Context, Service } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import compressionMiddleware from 'compression'
import Negotiator from 'negotiator'
import { renderIndexInjections, type IndexInjection } from './injections.ts'

export { renderIndexInjections } from './injections.ts'
export type { IndexInjection, IndexInjectionPlacement } from './injections.ts'

declare module '@deepseek-ai/cordis' {
  interface Context {
    webServer: WebServer
  }
  interface Events {
    /**
     * Collect the structured index injection table. Emitted on every index
     * render and every worker boot-payload request; listeners push their
     * current rows, so a row's data is read fresh at emit time.
     * @param table - Mutable row table; listeners append in activation order.
     * @mode emit
     */
    'webserver/index-inject'(table: IndexInjection[]): void
  }
}

/** Route match kind: 'exact' matches the pathname verbatim; 'prefix' p matches p and p/<anything>. */
export type WebRouteKind = 'exact' | 'prefix'

/** One named route registration. */
export interface WebRoute {
  kind: WebRouteKind
  /** Absolute pathname, no trailing slash. */
  path: string
  /** Owns the full response lifecycle (may hold the response open, e.g. SSE). */
  handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>
}

/** One exact-path HTTP upgrade registration. */
export interface WebUpgradeRoute {
  /** Absolute pathname, no trailing slash. */
  path: string
  /** Owns protocol negotiation and the upgraded socket after dispatch. */
  handler: (req: IncomingMessage, socket: Duplex, head: Buffer) => void | Promise<void>
}

/** Web server listen and response-compression config. */
export interface Config {
  /** Listen host; the two supported values are loopback and all-interfaces. */
  host: '127.0.0.1' | '0.0.0.0'
  /** Listen port; zero requests an OS-assigned port. */
  port: number
  /**
   * URL prefix this server is reverse-proxied under, with no trailing slash
   * (e.g. `/dsh-web`). A request outside the prefix is refused with 404; a
   * request inside it is dispatched with the prefix stripped from `req.url`
   * in place, so route handlers, the fallback owner, and upgrade handlers
   * all see an unprefixed path without needing basePath awareness of their
   * own. @default '' (served at the root)
   */
  basePath?: string
  /** Response compression for socket-backed HTTP requests. @default 'none' */
  compression?: 'none' | 'gzip'
  /** Gzip DEFLATE level from 0 through 9. @default 1 */
  compressionLevel?: number
  /** Minimum known response length eligible for gzip; unknown-length streams are eligible. @default 1024 */
  compressionThresholdBytes?: number
}

const DEFAULT_BASE_PATH = ''
const DEFAULT_COMPRESSION = 'none' as const
const DEFAULT_COMPRESSION_LEVEL = 1
const DEFAULT_COMPRESSION_THRESHOLD_BYTES = 1024

interface ResolvedConfig extends Config {
  basePath: string
  compression: 'none' | 'gzip'
  compressionLevel: number
  compressionThresholdBytes: number
}

/**
 * Fail loudly on a malformed `basePath`: anything but empty (root) or a
 * `/segment[/segment...]` path with no trailing slash is a misconfiguration a
 * deployment would otherwise discover only as a mysterious 404 loop.
 * @param basePath - the configured value, verbatim.
 */
function assertBasePath(basePath: string): void {
  if (basePath === '') return
  if (!basePath.startsWith('/') || basePath.endsWith('/') || basePath.includes('//') || /\s/.test(basePath)) {
    throw new Error(`webserver: basePath ${JSON.stringify(basePath)} must be empty or a "/segment[/segment...]" path with no trailing slash`)
  }
}

type NodeMiddleware = (
  req: IncomingMessage,
  res: ServerResponse,
  next: () => void,
) => void

function createGzipMiddleware(config: ResolvedConfig): NodeMiddleware {
  // `compression` is typed for Express, but its runtime uses only the
  // node:http request and response members supplied here.
  const middleware = compressionMiddleware({
    level: config.compressionLevel,
    threshold: config.compressionThresholdBytes,
    filter(request, response) {
      if (response.getHeader('content-range') !== undefined) return false
      const contentType = response.getHeader('content-type')
      if (typeof contentType === 'string' && contentType.toLowerCase().startsWith('text/event-stream')) return false
      return compressionMiddleware.filter(request, response)
    },
  }) as unknown as NodeMiddleware

  return (req, res, next) => {
    // The Web Worker tunnel has no socket and transfers identity bytes.
    if ((res as { socket?: unknown }).socket === undefined) {
      next()
      return
    }
    const encoding = new Negotiator(req).encoding(['gzip', 'identity'])
    const gzipRequest = Object.create(req) as IncomingMessage
    Object.defineProperty(gzipRequest, 'headers', {
      value: { ...req.headers, 'accept-encoding': encoding === 'gzip' ? 'gzip' : 'identity' },
    })
    middleware(gzipRequest, res, next)
  }
}

/**
 * The browser HTTP carrier service. Activation listens immediately. Route
 * registration order does not affect requests because configured named routes
 * must be distinct, and the fallback handler answers anything not yet claimed
 * during startup with 404 until its owner registers. A listen failure rejects
 * initialization, and the boot process reports the failed fiber.
 */
export class WebServer extends Service {
  static Config: z<Config> = z.object({
    host: z.union([z.const('127.0.0.1'), z.const('0.0.0.0')]).required(),
    port: z.natural().max(65535).required(),
    basePath: z.string().default(DEFAULT_BASE_PATH),
    compression: z.union([z.const('none'), z.const('gzip')]).default(DEFAULT_COMPRESSION),
    compressionLevel: z.number().step(1).min(0).max(9).default(DEFAULT_COMPRESSION_LEVEL),
    compressionThresholdBytes: z.natural().default(DEFAULT_COMPRESSION_THRESHOLD_BYTES),
  })

  private readonly exact = new Map<string, WebRoute>()
  private readonly prefixes = new Map<string, WebRoute>()
  private readonly upgrades = new Map<string, WebUpgradeRoute>()
  private readonly upgradedSockets = new Set<Duplex>()
  private readonly indexTaps: ((html: string) => string)[] = []
  private fallback: WebRoute['handler'] | undefined
  private server!: Server
  private listenedPort!: number
  private readonly gzip: NodeMiddleware | undefined
  private readonly basePath_: string

  constructor(ctx: Context, private config: Config) {
    super(ctx, 'webServer')
    const resolved = config as ResolvedConfig
    assertBasePath(resolved.basePath)
    this.basePath_ = resolved.basePath
    this.gzip = resolved.compression === 'gzip' ? createGzipMiddleware(resolved) : undefined
  }

  /** The listening port (the OS-assigned value when config.port is 0). */
  get port(): number {
    return this.listenedPort
  }

  /** The configured bind host (the loopback or all-interfaces literal). */
  get host(): Config['host'] {
    return this.config.host
  }

  /**
   * The configured `basePath` (no trailing slash, empty at the root). Every
   * request this class dispatches has already had it stripped, so this is
   * for consumers that must embed an absolute, browser-facing URL of their
   * own -- e.g. a `<script src>` outside the dist's relative-asset
   * convention -- and therefore need to restate the prefix explicitly.
   */
  get basePath(): string {
    return this.basePath_
  }

  /**
   * Register a named route. Duplicate (kind, path) throws — route patterns are
   * a composition-level contract, so a collision is a misconfiguration.
   * @param route - kind, path, and the owning handler.
   * @returns the disposer removing the route.
   */
  register(route: WebRoute): () => void {
    const table = route.kind === 'exact' ? this.exact : this.prefixes
    if (table.has(route.path)) {
      throw new Error(`webserver: duplicate ${route.kind} route "${route.path}"`)
    }
    table.set(route.path, route)
    return () => { table.delete(route.path) }
  }

  /**
   * Register an exact-path HTTP upgrade route. Duplicate paths throw because
   * one socket can have only one protocol owner.
   * @param route - pathname and handler owning negotiation plus socket use.
   * @returns the disposer removing the route.
   */
  registerUpgrade(route: WebUpgradeRoute): () => void {
    if (this.upgrades.has(route.path)) {
      throw new Error(`webserver: duplicate upgrade route "${route.path}"`)
    }
    this.upgrades.set(route.path, route)
    return () => { this.upgrades.delete(route.path) }
  }

  /**
   * Claim the fallback seat: the handler answering every request no named
   * route matches (the SPA dist server in the shipped Web composition). One
   * owner only — a second registration throws, because two fallbacks cannot
   * compose.
   * @param handler - owns the full response lifecycle of unmatched requests.
   * @returns the disposer releasing the seat.
   */
  registerFallback(handler: WebRoute['handler']): () => void {
    if (this.fallback !== undefined) {
      throw new Error('webserver: fallback already registered')
    }
    this.fallback = handler
    return () => { this.fallback = undefined }
  }

  /**
   * Register a raw-HTML index transform, the escape hatch for markup no
   * {@link IndexInjection} row expresses: {@link renderIndex} applies taps in
   * registration order after rendering the structured rows.
   * @param transform - pure html-to-html function.
   * @returns the disposer removing the transform.
   */
  tapIndex(transform: (html: string) => string): () => void {
    this.indexTaps.push(transform)
    return () => {
      const at = this.indexTaps.indexOf(transform)
      if (at !== -1) this.indexTaps.splice(at, 1)
    }
  }

  /** Listen; resolves once the socket is bound (rejection = FAILED fiber). */
  async [Service.init](): Promise<void> {
    const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
      const pathname = this.resolveDispatchPath(req)
      if (pathname === undefined) {
        res.writeHead(404)
        res.end()
        return
      }
      const route = this.match(pathname)
      if (route !== undefined) {
        await route.handler(req, res)
        return
      }
      const fallback = this.fallback
      if (fallback === undefined) {
        res.writeHead(404)
        res.end()
        return
      }
      await fallback(req, res)
    }
    // Last-resort guard: handle() rejecting would otherwise be an unhandled
    // rejection killing the process on one malformed request (bad %-escape,
    // client dropping mid-body). Per-request failures log and answer 400 —
    // never a process exit.
    this.server = createServer((req, res) => {
      const next = (): void => {
        void handle(req, res).catch((err: unknown) => {
          this.ctx.logger.warn(err instanceof Error ? err : new Error(String(err)))
          if (res.headersSent) {
            res.destroy()
            return
          }
          res.writeHead(400)
          res.end()
        })
      }
      if (this.gzip === undefined) next()
      else this.gzip(req, res, next)
    })
    this.server.on('upgrade', (req, socket, head) => {
      const onError = (error: Error): void => {
        this.ctx.logger.warn(error)
        socket.destroy()
      }
      socket.on('error', onError)
      socket.once('close', () => {
        socket.off('error', onError)
        this.upgradedSockets.delete(socket)
      })
      let route: WebUpgradeRoute | undefined
      try {
        const pathname = this.resolveDispatchPath(req)
        route = pathname === undefined ? undefined : this.upgrades.get(pathname)
      } catch (error) {
        this.ctx.logger.warn(error instanceof Error ? error : new Error(String(error)))
        socket.destroy()
        return
      }
      if (route === undefined) {
        socket.destroy()
        return
      }
      this.upgradedSockets.add(socket)
      try {
        Promise.resolve(route.handler(req, socket, head)).catch((error: unknown) => {
          this.ctx.logger.warn(error instanceof Error ? error : new Error(String(error)))
          socket.destroy()
        })
      } catch (error) {
        this.ctx.logger.warn(error instanceof Error ? error : new Error(String(error)))
        socket.destroy()
      }
    })

    await new Promise<void>((resolve, reject) => {
      this.server.once('error', reject)
      this.server.listen(this.config.port, this.config.host, () => {
        this.server.off('error', reject)
        this.server.on('error', (err) => { this.ctx.logger.error(err) })
        this.listenedPort = (this.server.address() as AddressInfo).port
        resolve()
      })
    })

    // Node does not include upgraded sockets in closeAllConnections(). The service
    // owns them with the other connections, so it tracks and destroys them explicitly.
    this.ctx.effect(() => async () => {
      const serverClosed = new Promise<void>((resolve) => {
        this.server.close(() => { resolve() })
      })
      this.server.closeAllConnections()
      const upgradedClosed = [...this.upgradedSockets].map(socket => new Promise<void>((resolve) => {
        socket.once('close', () => { resolve() })
        socket.destroy()
      }))
      await Promise.all([serverClosed, ...upgradedClosed])
    }, 'webServer.listen')
  }

  /**
   * Resolve the pathname to dispatch on for one request, stripping a
   * configured `basePath` prefix and rewriting `req.url` in place so every
   * downstream consumer that re-derives pathname from `req.url` (route
   * handlers, the fallback owner, upgrade handlers) sees an unprefixed path
   * without any basePath awareness of its own.
   * @param req - the incoming request or upgrade request.
   * @returns the unprefixed pathname, or undefined for a request outside
   * the configured mount point (basePath unset matches everything).
   */
  private resolveDispatchPath(req: IncomingMessage): string | undefined {
    /* v8 ignore next -- `?? '/'` arm: node:http always sets url on server
    requests; the field is only optional on the client-side IncomingMessage type */
    const parsed = new URL(req.url ?? '/', 'http://x')
    if (this.basePath_ === '') return parsed.pathname
    const stripped = parsed.pathname === this.basePath_
      ? '/'
      : parsed.pathname.startsWith(`${this.basePath_}/`) ? parsed.pathname.slice(this.basePath_.length) : undefined
    if (stripped !== undefined) req.url = stripped + parsed.search
    return stripped
  }

  /** Longest-prefix-wins over the prefix table after an exact-table miss. */
  private match(pathname: string): WebRoute | undefined {
    const exact = this.exact.get(pathname)
    if (exact !== undefined) return exact
    let best: WebRoute | undefined
    for (const [prefix, route] of this.prefixes) {
      if (pathname !== prefix && !pathname.startsWith(`${prefix}/`)) continue
      if (best === undefined || prefix.length > best.path.length) best = route
    }
    return best
  }

  /**
   * Run an index.html body through the registered taps in registration order
   * — called by the fallback owner on every index response it renders.
   * @param html - the raw index.html body.
   * @returns the transformed body.
   */
  applyIndexTaps(html: string): string {
    let out = html
    for (const transform of this.indexTaps) out = transform(out)
    return out
  }

  /**
   * Gather the structured injection table: one `webserver/index-inject` emit,
   * every subscriber pushes its current rows. Fresh per call, so subscribers
   * read live state (module graph, theme preference) at emit time.
   * @returns rows in subscriber activation order.
   */
  collectIndexInjections(): IndexInjection[] {
    const table: IndexInjection[] = []
    this.ctx.emit('webserver/index-inject', table)
    return table
  }

  /**
   * Render one index.html body: the structured injection table first, then
   * the raw `tapIndex` transforms over the result.
   * @param html - the raw index.html body.
   * @returns the transformed body.
   */
  renderIndex(html: string): string {
    return this.applyIndexTaps(renderIndexInjections(html, this.collectIndexInjections()))
  }
}

export default WebServer
