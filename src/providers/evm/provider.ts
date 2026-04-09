import {
  getAddress,
  keccak256,
  Log,
  parseEventLogs,
  ParseEventLogsReturnType,
  stringToBytes
} from 'viem';
import { BlockFetcher, FetchedBlock } from './fetchers/types';
import { Block, CustomJsonRpcError, EventsData, Writer } from './types';
import { CheckpointRecord } from '../../stores/checkpoints';
import { ContractSourceConfig } from '../../types';
import { BaseProvider, BlockNotFoundError, ReorgDetectedError } from '../base';

export class EvmProvider extends BaseProvider {
  private readonly fetcher: BlockFetcher;

  private readonly writers: Record<string, Writer>;
  private sourceHashes = new Map<string, string>();
  private logsCache = new Map<bigint, Log[]>();
  private blockCache = new Map<number, FetchedBlock>();

  constructor({
    instance,
    log,
    abis,
    writers,
    fetcher
  }: ConstructorParameters<typeof BaseProvider>[0] & {
    writers: Record<string, Writer>;
    fetcher: BlockFetcher;
  }) {
    super({ instance, log, abis });

    this.fetcher = fetcher;
    this.writers = writers;
  }

  formatAddresses(addresses: string[]): string[] {
    return addresses.map(address => getAddress(address));
  }

  async getNetworkIdentifier(): Promise<string> {
    const chainId = await this.fetcher.getChainId();

    return `evm_${chainId}`;
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.fetcher.getLatestBlockNumber();
  }

  async getBlockHash(blockNumber: number) {
    return this.fetcher.getBlockHash(blockNumber);
  }

  async processBlock(blockNumber: number, parentHash: string | null) {
    let block: Block | null = null;
    let eventsData: EventsData;

    const skipBlockFetching = this.instance.opts?.skipBlockFetching ?? false;
    const hasPreloadedBlockEvents =
      skipBlockFetching && this.logsCache.has(BigInt(blockNumber));

    try {
      if (!hasPreloadedBlockEvents) {
        const cached = this.blockCache.get(blockNumber);
        if (cached) {
          this.blockCache.delete(blockNumber);
          block = {
            number: BigInt(cached.number),
            hash: cached.hash,
            parentHash: cached.parentHash,
            timestamp: BigInt(cached.timestamp)
          } as Block;
        } else {
          const fetched = await this.fetcher.getBlock(blockNumber);
          block = {
            number: BigInt(fetched.number),
            hash: fetched.hash,
            parentHash: fetched.parentHash,
            timestamp: BigInt(fetched.timestamp)
          } as Block;
        }
      }
    } catch (err) {
      this.log.error({ blockNumber, err }, 'getting block failed... retrying');
      throw err;
    }

    if (!hasPreloadedBlockEvents && block === null) {
      this.log.info({ blockNumber }, 'block not found');
      throw new BlockNotFoundError();
    }

    try {
      eventsData = await this.getEvents({
        blockNumber: BigInt(blockNumber),
        blockHash: block?.hash ?? null
      });
    } catch (err: unknown) {
      if (err instanceof CustomJsonRpcError && err.code === -32000) {
        this.log.info({ blockNumber }, 'block events not found');
        throw new BlockNotFoundError();
      }

      this.log.error({ blockNumber, err }, 'getting events failed... retrying');
      throw err;
    }

    if (block && parentHash && block.parentHash !== parentHash) {
      this.log.error({ blockNumber }, 'reorg detected');
      throw new ReorgDetectedError();
    }

    await this.handleBlock(blockNumber, block, eventsData);

    if (block) {
      await this.instance.setBlockHash(blockNumber, block.hash);
    }

    await this.instance.setLastIndexedBlock(blockNumber);

    return blockNumber + 1;
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

    const helpers = this.instance.getWriterHelpers();

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
        const eventHash = event.topics[0];
        if (!eventHash) continue;

        const handler = globalEventHandlers[eventHash];
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

              let parsedEvent: ParseEventLogsReturnType[number] | undefined;
              if (source.abi && this.abis?.[source.abi]) {
                try {
                  const parsedLogs = parseEventLogs({
                    abi: this.abis[source.abi],
                    logs: [log]
                  });
                  parsedEvent =
                    parsedLogs[0] as ParseEventLogsReturnType[number];
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
    blockNumber: bigint;
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

      events = await this.fetcher.getLogsByBlockHash(blockHash);
    }

    return {
      isPreloaded,
      events: events.reduce((acc, event) => {
        if (event.transactionHash === null) return acc;

        if (!acc[event.transactionHash]) acc[event.transactionHash] = [];

        acc[event.transactionHash] = acc[event.transactionHash].concat(event);

        return acc;
      }, {})
    };
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

      const chunkEvents = await this.fetcher.getLogs(
        fromBlock,
        toBlock,
        address,
        [topics]
      );
      events = events.concat(chunkEvents);
    }

    return events;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number
  ): Promise<CheckpointRecord[]> {
    const { checkpoints, logs } = await this.fetcher.getCheckpointsRange(
      fromBlock,
      toBlock,
      this.instance.getCurrentSources(toBlock),
      name => this.getEventHash(name)
    );

    for (const log of logs) {
      if (log.blockNumber === null) continue;

      if (!this.logsCache.has(log.blockNumber)) {
        this.logsCache.set(log.blockNumber, []);
      }

      this.logsCache.get(log.blockNumber)?.push(log);
    }

    const cachedBlocks = this.fetcher.getCachedBlocks?.();
    if (cachedBlocks) {
      for (const [blockNumber, fetchedBlock] of cachedBlocks) {
        this.blockCache.set(blockNumber, fetchedBlock);
      }
      cachedBlocks.clear();
    }

    return checkpoints;
  }

  getEventHash(eventName: string) {
    if (!this.sourceHashes.has(eventName)) {
      this.sourceHashes.set(eventName, keccak256(stringToBytes(eventName)));
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
