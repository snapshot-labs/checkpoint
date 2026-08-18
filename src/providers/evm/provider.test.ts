import { createServer, Server } from 'http';
import { afterEach, describe, expect, it } from 'bun:test';
import { HyperSyncEvmProvider } from './hypersync-provider';
import { EvmProvider } from './provider';
import { Logger } from '../../utils/logger';
import { BlockNotFoundError, Instance } from '../base';

type JsonRpcResponse = { result: unknown } | { error: unknown };
type LogLine = { level: string; msg: string };

const servers: Server[] = [];

async function startRpcServer(response: JsonRpcResponse): Promise<string> {
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', chunk => (body += chunk));
    req.on('end', () => {
      const payload = JSON.parse(body);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: payload.id, ...response }));
    });
  });

  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));

  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('failed to start test rpc server');
  }

  return `http://127.0.0.1:${address.port}`;
}

function createTestLogger() {
  const lines: LogLine[] = [];
  const record = (level: string) => (_obj: unknown, msg: string) => {
    lines.push({ level, msg });
  };

  return {
    lines,
    log: {
      debug: record('debug'),
      info: record('info'),
      warn: record('warn'),
      error: record('error')
    } as unknown as Logger
  };
}

function createTestInstance(networkNodeUrl: string): Instance {
  return {
    config: { network_node_url: networkNodeUrl, sources: [] },
    opts: {},
    getCurrentSources: () => [],
    setBlockHash: async () => {},
    setLastIndexedBlock: async () => {},
    insertCheckpoints: async () => {},
    getWriterHelpers: () => ({ executeTemplate: async () => {} })
  } as unknown as Instance;
}

const MISSING_BLOCK: JsonRpcResponse = { result: null };

afterEach(() => {
  while (servers.length > 0) {
    servers.pop()?.close();
  }
});

describe('EvmProvider.processBlock', () => {
  it('should throw checkpoint BlockNotFoundError when the block is missing', async () => {
    const url = await startRpcServer(MISSING_BLOCK);
    const { lines, log } = createTestLogger();
    const provider = new EvmProvider({
      instance: createTestInstance(url),
      log,
      writers: {}
    });

    await expect(provider.processBlock(1000, null)).rejects.toBeInstanceOf(
      BlockNotFoundError
    );

    expect(lines).toContainEqual({ level: 'info', msg: 'block not found' });
    expect(lines.filter(line => line.level === 'error')).toEqual([]);
  });

  it('should rethrow other block fetching errors and log them at error level', async () => {
    const url = await startRpcServer({
      error: { code: -32602, message: 'invalid params' }
    });
    const { lines, log } = createTestLogger();
    const provider = new EvmProvider({
      instance: createTestInstance(url),
      log,
      writers: {}
    });

    await expect(provider.processBlock(1000, null)).rejects.not.toBeInstanceOf(
      BlockNotFoundError
    );

    expect(lines).toContainEqual({
      level: 'error',
      msg: 'getting block failed... retrying'
    });
  });
});

describe('HyperSyncEvmProvider.processBlock', () => {
  it('should throw checkpoint BlockNotFoundError when the block is missing from both cache and rpc', async () => {
    const url = await startRpcServer(MISSING_BLOCK);
    const { lines, log } = createTestLogger();
    const provider = new HyperSyncEvmProvider({
      instance: createTestInstance(url),
      log,
      writers: {},
      apiToken: 'test-token'
    });

    await expect(provider.processBlock(1000, null)).rejects.toBeInstanceOf(
      BlockNotFoundError
    );

    expect(lines.filter(line => line.level === 'error')).toEqual([]);
  });
});
