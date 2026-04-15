import { RPC } from 'starknet';
import {
  BasePreloaderParams,
  BaseWriterParams,
  PreloadTarget
} from '../../types';

// Shortcuts to starknet types.
export type Block = RPC.GetBlockWithTxHashesResponse;
export type PendingTransaction = RPC.PendingTransactions[number];
export type Event = RPC.GetEventsResponse['events'][number];

// (Partially) narrowed types as real types are not exported from `starknet`.
export type FullBlock = Block & { block_number: number; block_hash: string };

export type EventsData = {
  /**
   * Whether the events were preloaded from the cache.
   * If true, it means that events were fetched only for known sources and it might not contain all events from the block.
   * In that case additional logs fetching will be performed for new sources.
   */
  isPreloaded: boolean;
  events: Record<string, Event[]>;
};

export type ParsedEvent = Record<string, any>;

export type Writer = (
  args: {
    txId: string;
    block: FullBlock | null;
    rawEvent?: Event;
    event?: ParsedEvent;
  } & BaseWriterParams
) => Promise<void>;

export type Preloader = (
  args: {
    txId: string;
    block: FullBlock | null;
    rawEvent: Event;
    event?: ParsedEvent;
  } & BasePreloaderParams
) => PreloadTarget[] | Promise<PreloadTarget[]>;

export function isFullBlock(block: Block): block is FullBlock {
  return 'block_number' in block;
}
