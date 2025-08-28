import { Interface, LogDescription } from '@ethersproject/abi';
import { getAddress } from '@ethersproject/address';
import { keccak256 } from '@ethersproject/keccak256';
import {
  Formatter,
  Log,
  Provider,
  StaticJsonRpcProvider
} from '@ethersproject/providers';
import { toUtf8Bytes } from '@ethersproject/strings';
import { getRangeHint } from './helpers';
import { Block, CustomJsonRpcError, EventsData, Writer } from './types';
import { CheckpointRecord } from '../../stores/checkpoints';
import { ContractSourceConfig } from '../../types';
import { sleep } from '../../utils/helpers';
import { BaseProvider, BlockNotFoundError, ReorgDetectedError } from '../base';

type GetLogsBlockHashFilter = {
  blockHash: string;
};

type GetLogsBlockRangeFilter = {
  fromBlock: number;
  toBlock: number;
};

const MAX_BLOCKS_PER_REQUEST = 10000;

export class EvmProvider extends BaseProvider {
  private readonly provider: Provider;
  /**
   * Formatter instance from ethers.js used to format raw responses.
   */
  private readonly formatter = new Formatter();
  private readonly writers: Record<string, Writer>;
  private sourceHashes = new Map<string, string>();
  private logsCache = new Map<number, Log[]>();

  constructor({
    instance,
    log,
    abis,
    writers
  }: ConstructorParameters<typeof BaseProvider>[0] & {
    writers: Record<string, Writer>;
  }) {
    super({ instance, log, abis });

    this.provider = new StaticJsonRpcProvider(
      this.instance.config.network_node_url
    );
    this.writers = writers;
  }

  formatAddresses(addresses: string[]): string[] {
    return addresses.map(address => getAddress(address));
  }

  async getNetworkIdentifier(): Promise<string> {
    const result = await this.provider.getNetwork();
    return `evm_${result.chainId}`;
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.provider.getBlockNumber();
  }

  async getBlockHash(blockNumber: number) {
    const block = await this.provider.getBlock(blockNumber);
    return block.hash;
  }

  async processBlock(blockNum: number, parentHash: string | null) {
    let block: Block | null = null;
    let eventsData: EventsData;

    const skipBlockFetching = this.instance.opts?.skipBlockFetching ?? false;
    const hasPreloadedBlockEvents =
      skipBlockFetching && this.logsCache.has(blockNum);

    try {
      if (!hasPreloadedBlockEvents) {
        block = await this.provider.getBlock(blockNum);
      }
    } catch (e) {
      this.log.error(
        { blockNumber: blockNum, err: e },
        'getting block failed... retrying'
      );
      throw e;
    }

    if (!hasPreloadedBlockEvents && block === null) {
      this.log.info({ blockNumber: blockNum }, 'block not found');
      throw new BlockNotFoundError();
    }

    try {
      eventsData = await this.getEvents({
        blockNumber: blockNum,
        blockHash: block?.hash ?? null
      });
    } catch (e: unknown) {
      if (e instanceof CustomJsonRpcError && e.code === -32000) {
        this.log.info({ blockNumber: blockNum }, 'block events not found');
        throw new BlockNotFoundError();
      }

      this.log.error(
        { blockNumber: blockNum, err: e },
        'getting events failed... retrying'
      );
      throw e;
    }

    if (block && parentHash && block.parentHash !== parentHash) {
      this.log.error({ blockNumber: blockNum }, 'reorg detected');
      throw new ReorgDetectedError();
    }

    await this.handleBlock(blockNum, block, eventsData);

    if (block) {
      await this.instance.setBlockHash(blockNum, block.hash);
    }

    await this.instance.setLastIndexedBlock(blockNum);

    return blockNum + 1;
  }

  private async handleBlock(
    blockNumber: number,
    block: Block | null,
    eventsData: EventsData
  ) {
    this.log.info({ blockNumber }, 'handling block');

    const blockTransactions = Object.keys(eventsData.events);

    for (const txId of blockTransactions) {
      await this.handleTx(
        block,
        blockNumber,
        txId,
        eventsData.isPreloaded,
        eventsData.events[txId] || []
      );
    }

    this.log.debug({ blockNumber }, 'handling block done');
  }

  private async handleTx(
    block: Block | null,
    blockNumber: number,
    txId: string,
    isPreloaded: boolean,
    logs: Log[]
  ) {
    this.log.debug({ txId }, 'handling transaction');

    const helpers = await this.instance.getWriterHelpers();

    if (this.instance.config.tx_fn) {
      await this.writers[this.instance.config.tx_fn]({
        blockNumber,
        block,
        txId,
        helpers
      });
    }

    if (this.instance.config.global_events) {
      const globalEventHandlers = this.instance.config.global_events.reduce(
        (handlers, event) => {
          handlers[this.getEventHash(event.name)] = {
            name: event.name,
            fn: event.fn
          };
          return handlers;
        },
        {}
      );

      for (const event of logs) {
        const handler = globalEventHandlers[event.topics[0]];
        if (!handler) continue;

        this.log.info(
          {
            contract: event.address,
            event: handler.name,
            handlerFn: handler.fn
          },
          'found contract event'
        );

        await this.writers[handler.fn]({
          block,
          blockNumber,
          txId,
          rawEvent: event,
          helpers
        });
      }
    }

    let lastSources = this.instance.getCurrentSources(blockNumber);
    let sourcesQueue = [...lastSources];

    let source: ContractSourceConfig | undefined;
    while ((source = sourcesQueue.shift())) {
      let foundContractData = false;
      for (const log of logs) {
        if (this.compareAddress(source.contract, log.address)) {
          for (const sourceEvent of source.events) {
            const targetTopic = this.getEventHash(sourceEvent.name);

            if (targetTopic === log.topics[0]) {
              foundContractData = true;
              this.log.info(
                {
                  contract: source.contract,
                  event: sourceEvent.name,
                  handlerFn: sourceEvent.fn
                },
                'found contract event'
              );

              let parsedEvent: LogDescription | undefined;
              if (source.abi && this.abis?.[source.abi]) {
                const iface = new Interface(this.abis[source.abi]);
                try {
                  parsedEvent = iface.parseLog(log);
                } catch (err) {
                  this.log.warn(
                    {
                      err,
                      contract: source.contract,
                      txId,
                      handlerFn: sourceEvent.fn
                    },
                    'failed to parse event'
                  );
                }
              }

              await this.writers[sourceEvent.fn]({
                source,
                block,
                blockNumber,
                txId,
                rawEvent: log,
                event: parsedEvent,
                helpers
              });
            }
          }
        }
      }

      if (foundContractData) {
        await this.instance.insertCheckpoints([
          { blockNumber, contractAddress: getAddress(source.contract) }
        ]);

        const nextSources = this.instance.getCurrentSources(blockNumber);
        const newSources = nextSources.slice(lastSources.length);

        sourcesQueue = sourcesQueue.concat(newSources);
        lastSources = this.instance.getCurrentSources(blockNumber);

        if (isPreloaded && newSources.length) {
          this.handleNewSourceAdded();

          this.log.info(
            { newSources: newSources.map(s => s.contract) },
            'new sources added, fetching missing logs'
          );

          const newSourcesLogs = await this.getLogsForSources({
            fromBlock: blockNumber,
            toBlock: blockNumber,
            sources: newSources
          });

          logs = logs.concat(newSourcesLogs);
        }
      }
    }

    this.log.debug({ txId }, 'handling transaction done');
  }

  private async getEvents({
    blockHash,
    blockNumber
  }: {
    blockHash: string | null;
    blockNumber: number;
  }): Promise<EventsData> {
    let isPreloaded = false;
    let events: Log[] = [];

    if (this.logsCache.has(blockNumber)) {
      isPreloaded = true;
      events = this.logsCache.get(blockNumber) || [];
      this.logsCache.delete(blockNumber);
    } else {
      if (!blockHash) {
        throw new Error('Block hash is required to fetch logs from network');
      }

      events = await this._getLogs({
        blockHash
      });
    }

    return {
      isPreloaded,
      events: events.reduce((acc, event) => {
        if (!acc[event.transactionHash]) acc[event.transactionHash] = [];

        acc[event.transactionHash] = acc[event.transactionHash].concat(event);

        return acc;
      }, {})
    };
  }

  /**
   * This method is simpler implementation of getLogs method.
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

    if ('blockHash' in filter) {
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

    const res = await fetch(this.instance.config.network_node_url, {
      method: 'POST',
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

    return Formatter.arrayOf(this.formatter.filterLog.bind(this.formatter))(
      json.result
    );
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
          this.log.warn(
            { err, rangeHint, fromBlock: currentFrom, toBlock: currentTo },
            'getLogs failed. Received new range hint'
          );

          currentFrom = rangeHint.from;
          currentTo = rangeHint.to;
          continue;
        }

        this.log.error(
          { fromBlock: currentFrom, toBlock: currentTo, address, err },
          'getLogs failed'
        );

        await sleep(5000);
      }
    }

    return result;
  }

  async getLogsForSources({
    fromBlock,
    toBlock,
    sources
  }: {
    fromBlock: number;
    toBlock: number;
    sources: ContractSourceConfig[];
  }): Promise<Log[]> {
    const chunks: ContractSourceConfig[][] = [];
    for (let i = 0; i < sources.length; i += 20) {
      chunks.push(sources.slice(i, i + 20));
    }

    let events: Log[] = [];
    for (const chunk of chunks) {
      const address = chunk.map(source => source.contract);
      const topics = chunk.flatMap(source =>
        source.events.map(event => this.getEventHash(event.name))
      );

      const chunkEvents = await this.getLogs(fromBlock, toBlock, address, [
        topics
      ]);
      events = events.concat(chunkEvents);
    }

    return events;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number
  ): Promise<CheckpointRecord[]> {
    const events = await this.getLogsForSources({
      fromBlock,
      toBlock,
      sources: this.instance.getCurrentSources(fromBlock)
    });

    for (const log of events) {
      if (!this.logsCache.has(log.blockNumber)) {
        this.logsCache.set(log.blockNumber, []);
      }

      this.logsCache.get(log.blockNumber)?.push(log);
    }

    return events.map(log => ({
      blockNumber: log.blockNumber,
      contractAddress: log.address
    }));
  }

  getEventHash(eventName: string) {
    if (!this.sourceHashes.has(eventName)) {
      this.sourceHashes.set(eventName, keccak256(toUtf8Bytes(eventName)));
    }

    return this.sourceHashes.get(eventName) as string;
  }

  compareAddress(a: string, b: string) {
    return a.toLowerCase() === b.toLowerCase();
  }

  handleNewSourceAdded(): void {
    this.log.info('new source added, clearing logs cache');
    this.logsCache.clear();
  }
}
