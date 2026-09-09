import { createServer, Server } from 'http';
import { afterEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { InvalidParamsRpcError } from 'viem';
import { HyperSyncEvmProvider } from './hypersync-provider';
import { EvmProvider } from './provider';
import { createLogger } from '../../utils/logger';
import { BlockNotFoundError, Instance, RpcSelector } from '../base';

type JsonRpcResponse = { result: unknown } | { error: unknown };

const MISSING_BLOCK: JsonRpcResponse = { result: null };

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

async function createProviderFixture(response: JsonRpcResponse) {
  const url = await startRpcServer(response);
  const log = createLogger({ level: 'silent' });

  return {
    infoSpy: spyOn(log, 'info'),
    errorSpy: spyOn(log, 'error'),
    params: {
      instance: { config: { network_node_url: url } } as unknown as Instance,
      log,
      writers: {}
    }
  };
}

afterEach(() => {
  for (const server of servers.splice(0)) {
    server.close();
  }
});

describe('EvmProvider.getBlockHash', () => {
  it('should throw checkpoint BlockNotFoundError when the block is missing', async () => {
    const { params } = await createProviderFixture(MISSING_BLOCK);
    const provider = new EvmProvider(params);

    await expect(provider.getBlockHash(1000)).rejects.toBeInstanceOf(
      BlockNotFoundError
    );
  });
});

describe('EvmProvider.processBlock', () => {
  it('should throw checkpoint BlockNotFoundError when the block is missing', async () => {
    const { infoSpy, errorSpy, params } =
      await createProviderFixture(MISSING_BLOCK);
    const provider = new EvmProvider(params);

    await expect(provider.processBlock(1000, null)).rejects.toBeInstanceOf(
      BlockNotFoundError
    );

    expect(infoSpy).toHaveBeenCalledWith(
      { blockNumber: 1000 },
      'block not found'
    );
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it('should rethrow other block fetching errors and log them at error level', async () => {
    const { errorSpy, params } = await createProviderFixture({
      error: { code: -32602, message: 'invalid params' }
    });
    const provider = new EvmProvider(params);

    await expect(provider.processBlock(1000, null)).rejects.toBeInstanceOf(
      InvalidParamsRpcError
    );

    expect(errorSpy).toHaveBeenCalledWith(
      expect.objectContaining({ blockNumber: 1000 }),
      'getting block failed... retrying'
    );
  });
});

describe('EvmProvider rpcSelector', () => {
  it('should pick url per request', async () => {
    const urlA = await startRpcServer({ result: '0x1' });
    const urlB = await startRpcServer({ result: '0x2' });
    const rpcSelector = mock<RpcSelector>(context =>
      context.type === 'getBlockNumber' ? urlB : urlA
    );
    const provider = new EvmProvider({
      instance: { config: { network_node_url: urlA } } as unknown as Instance,
      log: createLogger({ level: 'silent' }),
      writers: {},
      rpcSelector
    });

    expect(await provider.getNetworkIdentifier()).toBe('evm_1');
    expect(await provider.getLatestBlockNumber()).toBe(2);
    expect(await provider.getNetworkIdentifier()).toBe('evm_1');
  });
});

describe('HyperSyncEvmProvider.processBlock', () => {
  it('should throw checkpoint BlockNotFoundError when the block is missing from rpc (cache empty)', async () => {
    const { errorSpy, params } = await createProviderFixture(MISSING_BLOCK);
    const provider = new HyperSyncEvmProvider({
      ...params,
      apiToken: 'test-token'
    });

    await expect(provider.processBlock(1000, null)).rejects.toBeInstanceOf(
      BlockNotFoundError
    );

    expect(errorSpy).not.toHaveBeenCalled();
  });
});
