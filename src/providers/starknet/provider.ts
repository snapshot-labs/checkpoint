import { BLOCK_ID, BlockWithTxReceipts } from '@starknet-io/types-js';
import { hash, RpcProvider, validateAndParseAddress } from 'starknet';
import { BaseProvider, BlockNotFoundError, ReorgDetectedError } from '../base';
import {
  Block,
  Event,
  EventsData,
  FullBlock,
  isFullBlock,
  ParsedEvent,
  Writer
} from './types';
import { parseEvent } from './utils';
import { CheckpointRecord } from '../../stores/checkpoints';
import { ContractSourceConfig } from '../../types';
import { sleep } from '../../utils/helpers';

class CustomJsonRpcError extends Error {
  constructor(
    message: string,
    public code: number,
    public data: any
  ) {
    super(message);
  }
}

export class StarknetProvider extends BaseProvider {
  private readonly provider: RpcProvider;
  private readonly writers: Record<string, Writer>;
  private seenPoolTransactions = new Set();
  private processedTransactions = new Set();
  private sourceHashes = new Map<string, string>();
  private logsCache = new Map<number, Event[]>();

  constructor({
    instance,
    log,
    abis,
    writers
  }: ConstructorParameters<typeof BaseProvider>[0] & {
    writers: Record<string, Writer>;
  }) {
    super({ instance, log, abis });

    this.provider = new RpcProvider({
      nodeUrl: this.instance.config.network_node_url
    });
    this.writers = writers;
  }

  formatAddresses(addresses: string[]): string[] {
    return addresses.map(address => validateAndParseAddress(address));
  }

  async getNetworkIdentifier(): Promise<string> {
    const result = await this.provider.getChainId();
    return `starknet_${result}`;
  }

  async getLatestBlockNumber(): Promise<number> {
    return this.provider.getBlockNumber();
  }

  async getBlockHash(blockNumber: number) {
    const block = await this.provider.getBlock(blockNumber);
    return block.block_hash;
  }

  async processBlock(blockNum: number, parentHash: string | null) {
    let block: Block | null = null;
    let eventsData: EventsData;
    try {
      const skipBlockFetching = this.instance.opts?.skipBlockFetching ?? false;
      const hasPreloadedBlockEvents =
        skipBlockFetching && this.logsCache.has(blockNum);

      if (!hasPreloadedBlockEvents) {
        block = await this.provider.getBlockWithTxHashes(blockNum);
      }

      if (block && (!isFullBlock(block) || block.block_number !== blockNum)) {
        this.log.error({ blockNumber: blockNum }, 'invalid block');
        throw new Error('invalid block');
      }

      eventsData = await this.getEvents({
        blockNumber: blockNum,
        blockHash: block?.block_hash ?? null
      });
    } catch (e) {
      if ((e as Error).message.includes('Block not found')) {
        this.log.info({ blockNumber: blockNum }, 'block not found');
        throw new BlockNotFoundError();
      }

      this.log.error(
        { blockNumber: blockNum, err: e },
        'getting block failed... retrying'
      );
      throw e;
    }

    if (block && parentHash && block.parent_hash !== parentHash) {
      this.log.error({ blockNumber: blockNum }, 'reorg detected');
      throw new ReorgDetectedError();
    }

    await this.handleBlock(blockNum, block, eventsData);

    if (block && isFullBlock(block)) {
      await this.instance.setBlockHash(blockNum, block.block_hash);
    }

    await this.instance.setLastIndexedBlock(blockNum);

    return blockNum + 1;
  }

  async processPool(blockNumber: number) {
    const block = await this.getBlockWithReceipts('pre_confirmed');
    const receipts = block.transactions
      .map(({ receipt }) => receipt)
      .filter(
        receipt => !this.seenPoolTransactions.has(receipt.transaction_hash)
      );

    const txIds = receipts.map(receipt => receipt.transaction_hash);

    const eventsData = {
      isPreloaded: false,
      events: receipts.reduce((acc, receipt) => {
        if (receipt === null) return acc;

        acc[receipt.transaction_hash] = receipt.events;
        return acc;
      }, {})
    };

    await this.handlePool(txIds, eventsData, blockNumber);
  }

  private async handleBlock(
    blockNumber: number,
    block: FullBlock | null,
    eventsData: EventsData
  ) {
    this.log.info({ blockNumber }, 'handling block');

    const blockTransactions = Object.keys(eventsData.events);
    const txsToCheck = blockTransactions.filter(
      txId => !this.seenPoolTransactions.has(txId)
    );

    for (const txId of txsToCheck) {
      await this.handleTx(
        block,
        blockNumber,
        txId,
        eventsData.isPreloaded,
        eventsData.events[txId] || []
      );
    }

    this.seenPoolTransactions.clear();

    this.log.debug({ blockNumber }, 'handling block done');
  }

  private async handlePool(
    txIds: string[],
    eventsData: EventsData,
    blockNumber: number
  ) {
    this.log.info('handling pool');

    for (const txId of txIds) {
      await this.handleTx(
        null,
        blockNumber,
        txId,
        eventsData.isPreloaded,
        eventsData.events[txId] || []
      );

      this.seenPoolTransactions.add(txId);
    }

    this.log.info('handling pool done');
  }

  private async handleTx(
    block: FullBlock | null,
    blockNumber: number,
    txId: string,
    isPreloaded: boolean,
    events: Event[]
  ) {
    this.log.debug({ txId }, 'handling transaction');

    if (this.processedTransactions.has(txId)) {
      this.log.warn({ hash: txId }, 'transaction already processed');
      return;
    }

    let wasTransactionProcessed = false;
    const helpers = await this.instance.getWriterHelpers();

    if (this.instance.config.tx_fn) {
      await this.writers[this.instance.config.tx_fn]({
        blockNumber,
        block,
        txId,
        helpers
      });

      wasTransactionProcessed = true;
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

      for (const event of events) {
        const handler = globalEventHandlers[event.keys[0]];
        if (!handler) continue;

        this.log.info(
          {
            contract: event.from_address,
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

        wasTransactionProcessed = true;
      }
    }

    let lastSources = this.instance.getCurrentSources(blockNumber);
    let sourcesQueue = [...lastSources];

    let source: ContractSourceConfig | undefined;
    while ((source = sourcesQueue.shift())) {
      let foundContractData = false;
      const contract = validateAndParseAddress(source.contract);

      for (const event of events) {
        if (contract === validateAndParseAddress(event.from_address)) {
          for (const sourceEvent of source.events) {
            if (this.getEventHash(sourceEvent.name) === event.keys[0]) {
              foundContractData = true;
              this.log.info(
                {
                  contract: source.contract,
                  event: sourceEvent.name,
                  handlerFn: sourceEvent.fn
                },
                'found contract event'
              );

              let parsedEvent: ParsedEvent | undefined;
              if (source.abi && this.abis?.[source.abi]) {
                try {
                  parsedEvent = parseEvent(this.abis[source.abi], event);
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
                rawEvent: event,
                event: parsedEvent,
                helpers
              });

              wasTransactionProcessed = true;
            }
          }
        }
      }

      if (wasTransactionProcessed) {
        this.processedTransactions.add(txId);
      }

      if (foundContractData) {
        await this.instance.insertCheckpoints([
          {
            blockNumber,
            contractAddress: validateAndParseAddress(source.contract)
          }
        ]);

        const nextSources = this.instance.getCurrentSources(blockNumber);
        const newSources = nextSources.slice(lastSources.length);

        sourcesQueue = sourcesQueue.concat(newSources);
        lastSources = this.instance.getCurrentSources(blockNumber);

        if (isPreloaded && newSources.length > 0) {
          this.handleNewSourceAdded();

          this.log.info(
            { newSources: newSources.map(s => s.contract) },
            'new sources added, fetching missing events'
          );

          const newSourcesEvents = await this.getEventsForSources({
            fromBlock: blockNumber,
            toBlock: blockNumber,
            sources: newSources
          });

          events = events.concat(newSourcesEvents);
        }
      }
    }

    this.log.debug({ txId }, 'handling transaction done');
  }

  private async getBlockWithReceipts(blockId: BLOCK_ID) {
    const res = await fetch(this.instance.config.network_node_url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'starknet_getBlockWithReceipts',
        params: [blockId]
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

    return json.result as BlockWithTxReceipts;
  }

  private async getEvents({
    blockHash,
    blockNumber
  }: {
    blockHash: string | null;
    blockNumber: number;
  }): Promise<EventsData> {
    let isPreloaded = false;
    let events: Event[] = [];

    if (this.logsCache.has(blockNumber)) {
      isPreloaded = true;
      events = this.logsCache.get(blockNumber) || [];
      this.logsCache.delete(blockNumber);
    } else {
      if (!blockHash) {
        throw new Error('Block hash is required to fetch logs from network');
      }

      let continuationToken: string | undefined;
      do {
        const result = await this.provider.getEvents({
          from_block: { block_hash: blockHash },
          to_block: { block_hash: blockHash },
          chunk_size: 1000,
          continuation_token: continuationToken
        });

        events = events.concat(result.events);

        continuationToken = result.continuation_token;
      } while (continuationToken);
    }

    return {
      isPreloaded,
      events: events.reduce((acc, event) => {
        if (!acc[event.transaction_hash]) acc[event.transaction_hash] = [];

        acc[event.transaction_hash].push(event);

        return acc;
      }, {})
    };
  }

  async getEventsRangeForAddress(
    fromBlock: number,
    toBlock: number,
    address: string,
    eventNames: string[]
  ): Promise<Event[]> {
    let events: Event[] = [];

    let continuationToken: string | undefined;
    do {
      try {
        const result = await this.provider.getEvents({
          from_block: { block_number: fromBlock },
          to_block: { block_number: toBlock },
          address: address,
          keys: [eventNames.map(name => this.getEventHash(name))],
          chunk_size: 1000,
          continuation_token: continuationToken
        });

        events = events.concat(result.events);

        continuationToken = result.continuation_token;
      } catch (e) {
        this.log.error(
          { fromBlock, toBlock, continuationToken, address, err: e },
          'getEvents failed'
        );

        await sleep(5000);
      }
    } while (continuationToken);

    return events;
  }

  async getEventsForSources({
    fromBlock,
    toBlock,
    sources
  }: {
    fromBlock: number;
    toBlock: number;
    sources: ContractSourceConfig[];
  }): Promise<Event[]> {
    let events: Event[] = [];

    for (const source of sources) {
      const addressEvents = await this.getEventsRangeForAddress(
        fromBlock,
        toBlock,
        source.contract,
        source.events.map(event => event.name)
      );
      events = events.concat(addressEvents);
    }

    return events;
  }

  async getCheckpointsRange(
    fromBlock: number,
    toBlock: number
  ): Promise<CheckpointRecord[]> {
    const events = await this.getEventsForSources({
      fromBlock,
      toBlock,
      sources: this.instance.getCurrentSources(toBlock)
    });

    for (const log of events) {
      if (!this.logsCache.has(log.block_number)) {
        this.logsCache.set(log.block_number, []);
      }

      this.logsCache.get(log.block_number)?.push(log);
    }

    return events.map(event => ({
      blockNumber: event.block_number,
      contractAddress: validateAndParseAddress(event.from_address)
    }));
  }

  getEventHash(eventName: string) {
    if (!this.sourceHashes.has(eventName)) {
      this.sourceHashes.set(
        eventName,
        `0x${hash.starknetKeccak(eventName).toString(16)}`
      );
    }

    return this.sourceHashes.get(eventName) as string;
  }

  handleNewSourceAdded(): void {
    this.log.info('new source added, clearing logs cache');
    this.logsCache.clear();
  }
}
