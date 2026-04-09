import {
  createPublicClient,
  formatLog,
  http,
  Log,
  PublicClient,
  RpcLog
} from 'viem';
import { BlockFetcher, FetchedBlock } from './types';
import { getRangeHint } from '../helpers';
import { CustomJsonRpcError } from '../types';
import { CheckpointRecord } from '../../../stores/checkpoints';
import { ContractSourceConfig } from '../../../types';
import { sleep } from '../../../utils/helpers';

type GetLogsBlockHashFilter = {
  blockHash: string;
};

type GetLogsBlockRangeFilter = {
  fromBlock: number;
  toBlock: number;
};

/**
 * Timeout for client requests in milliseconds.
 * This timeout is also used when fetching latest blocks in getLogs.
 */
const CLIENT_TIMEOUT = 5 * 1000;

const MAX_BLOCKS_PER_REQUEST = 10000;

export class RpcBlockFetcher implements BlockFetcher {
  private readonly client: PublicClient;
  private readonly networkNodeUrl: string;

  constructor(networkNodeUrl: string) {
    this.networkNodeUrl = networkNodeUrl;
    this.client = createPublicClient({
      transport: http(networkNodeUrl, {
        timeout: CLIENT_TIMEOUT
      })
    });
  }

  async getChainId(): Promise<number> {
    return this.client.getChainId();
  }

  async getLatestBlockNumber(): Promise<number> {
    const blockNumber = await this.client.getBlockNumber();

    return Number(blockNumber);
  }

  async getBlock(blockNumber: number): Promise<FetchedBlock> {
    const block = await this.client.getBlock({
      blockNumber: BigInt(blockNumber)
    });

    return {
      number: Number(block.number),
      hash: block.hash,
      parentHash: block.parentHash,
      timestamp: Number(block.timestamp)
    };
  }

  async getBlockHash(blockNumber: number): Promise<string> {
    const block = await this.client.getBlock({
      blockNumber: BigInt(blockNumber)
    });

    return block.hash;
  }

  /**
   * This method is a simpler implementation of getLogs method.
   * This allows using two filters that are not supported in ethers v5:
   * - `blockHash` to get logs for a specific block - if node doesn't know about that block it will fail.
   * - `address` as a single address or an array of addresses.
   * @param filter Logs filter
   */
  private async _getLogs(
    filter: (GetLogsBlockHashFilter | GetLogsBlockRangeFilter) & {
      address?: string | string[];
      topics?: (string | string[])[];
    }
  ): Promise<Log[]> {
    const params: {
      fromBlock?: string;
      toBlock?: string;
      blockHash?: string;
      address?: string | string[];
      topics?: (string | string[])[];
    } = {};

    let signal: AbortSignal | undefined;

    if ('blockHash' in filter) {
      signal = AbortSignal.timeout(CLIENT_TIMEOUT);
      params.blockHash = filter.blockHash;
    }

    if ('fromBlock' in filter) {
      params.fromBlock = `0x${filter.fromBlock.toString(16)}`;
    }

    if ('toBlock' in filter) {
      params.toBlock = `0x${filter.toBlock.toString(16)}`;
    }

    if ('address' in filter) {
      params.address = filter.address;
    }

    if ('topics' in filter) {
      params.topics = filter.topics;
    }

    const res = await fetch(this.networkNodeUrl, {
      method: 'POST',
      signal,
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'eth_getLogs',
        params: [params]
      })
    });

    if (!res.ok) {
      throw new Error(`Request failed: ${res.statusText}`);
    }

    const json = await res.json();

    if (json.error) {
      throw new CustomJsonRpcError(
        json.error.message,
        json.error.code,
        json.error.data
      );
    }

    return json.result.map((log: RpcLog) => formatLog(log));
  }

  async getLogsByBlockHash(blockHash: string): Promise<Log[]> {
    return this._getLogs({ blockHash });
  }

  async getLogs(
    fromBlock: number,
    toBlock: number,
    address: string | string[],
    topics: (string | string[])[] = []
  ): Promise<Log[]> {
    let result = [] as Log[];

    let currentFrom = fromBlock;
    let currentTo = Math.min(toBlock, currentFrom + MAX_BLOCKS_PER_REQUEST);
    while (true) {
      try {
        const logs = await this._getLogs({
          fromBlock: currentFrom,
          toBlock: currentTo,
          address,
          topics
        });

        result = result.concat(logs);

        if (currentTo === toBlock) break;
        currentFrom = currentTo + 1;
        currentTo = Math.min(toBlock, currentFrom + MAX_BLOCKS_PER_REQUEST);
      } catch (err: unknown) {
        const rangeHint = getRangeHint(err, {
          from: currentFrom,
          to: currentTo
        });

        if (rangeHint) {
          currentFrom = rangeHint.from;
          currentTo = rangeHint.to;
          continue;
        }

        await sleep(5000);
      }
    }

    return result;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number,
    sources: ContractSourceConfig[],
    getEventHash: (name: string) => string
  ): Promise<{ checkpoints: CheckpointRecord[]; logs: Log[] }> {
    const chunks: ContractSourceConfig[][] = [];
    for (let i = 0; i < sources.length; i += 20) {
      chunks.push(sources.slice(i, i + 20));
    }

    let logs: Log[] = [];
    for (const chunk of chunks) {
      const address = chunk.map(source => source.contract);
      const topics = chunk.flatMap(source =>
        source.events.map(event => getEventHash(event.name))
      );

      const chunkLogs = await this.getLogs(fromBlock, toBlock, address, [
        topics
      ]);
      logs = logs.concat(chunkLogs);
    }

    const checkpoints = logs.map(log => ({
      blockNumber: Number(log.blockNumber),
      contractAddress: log.address
    }));

    return { checkpoints, logs };
  }
}
