import { Knex } from 'knex';
import {
  CheckpointRecord,
  CheckpointsStore,
  MetadataId
} from '../stores/checkpoints';
import { PreloadTarget } from '../types';
import { chunk } from '../utils/helpers';

type Row = Record<string, any>;

type EntityState = {
  loaded: Row | null;
  hydrated: boolean;
  dirty: Row | null;
  deleted: boolean;
  firstChangeBlock: number | null;
};

const UPDATE_CHUNK_SIZE = 1000;
const INSERT_CHUNK_SIZE = 1000;

export class EntityWriteBuffer {
  private readonly indexerName: string;
  private readonly knex: Knex;
  private readonly store: CheckpointsStore;

  private cache = new Map<string, Map<string, EntityState>>();
  private checkpoints: CheckpointRecord[] = [];
  private blockHashes = new Map<number, string>();

  constructor(indexerName: string, knex: Knex, store: CheckpointsStore) {
    this.indexerName = indexerName;
    this.knex = knex;
    this.store = store;
  }

  private getTableMap(tableName: string): Map<string, EntityState> {
    let table = this.cache.get(tableName);
    if (!table) {
      table = new Map();
      this.cache.set(tableName, table);
    }
    return table;
  }

  private key(id: string | number): string {
    return String(id);
  }

  async loadEntity(
    tableName: string,
    id: string | number
  ): Promise<Row | null> {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      if (existing.deleted) return null;
      if (existing.dirty) return existing.dirty;
      if (existing.hydrated) return existing.loaded;
    }

    const row = await this.knex
      .table(tableName)
      .select('*')
      .where('id', id)
      .andWhere('_indexer', this.indexerName)
      .andWhereRaw('upper_inf(block_range)')
      .first();

    const loaded = row ?? null;
    table.set(key, {
      loaded,
      hydrated: true,
      dirty: null,
      deleted: false,
      firstChangeBlock: null
    });

    return loaded;
  }

  stageInsert(
    tableName: string,
    id: string | number,
    row: Row,
    blockNumber: number
  ) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.dirty = row;
      existing.deleted = false;
      if (existing.firstChangeBlock === null) {
        existing.firstChangeBlock = blockNumber;
      }
      return;
    }

    table.set(key, {
      loaded: null,
      hydrated: true,
      dirty: row,
      deleted: false,
      firstChangeBlock: blockNumber
    });
  }

  stageUpdate(
    tableName: string,
    id: string | number,
    row: Row,
    blockNumber: number
  ) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.dirty = row;
      existing.deleted = false;
      if (existing.firstChangeBlock === null) {
        existing.firstChangeBlock = blockNumber;
      }
      return;
    }

    // Save called on a model marked `exists` but never loaded via the buffer.
    // Force a DB close-range UPDATE alongside the INSERT; if the row doesn't
    // actually exist the UPDATE is a no-op.
    table.set(key, {
      loaded: { id } as Row,
      hydrated: true,
      dirty: row,
      deleted: false,
      firstChangeBlock: blockNumber
    });
  }

  stageDelete(tableName: string, id: string | number, blockNumber: number) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.deleted = true;
      if (existing.firstChangeBlock === null) {
        existing.firstChangeBlock = blockNumber;
      }
      return;
    }

    table.set(key, {
      loaded: { id } as Row,
      hydrated: true,
      dirty: null,
      deleted: true,
      firstChangeBlock: blockNumber
    });
  }

  pushCheckpoints(records: CheckpointRecord[]) {
    if (records.length === 0) return;
    this.checkpoints.push(...records);
  }

  pushBlockHash(blockNumber: number, hash: string) {
    this.blockHashes.set(blockNumber, hash);
  }

  getPendingBlockHash(blockNumber: number): string | null {
    return this.blockHashes.get(blockNumber) ?? null;
  }

  async prefetch(targets: PreloadTarget[]): Promise<void> {
    if (targets.length === 0) return;

    const uncachedByTable = new Map<string, Set<string | number>>();
    for (const target of targets) {
      const table = this.getTableMap(target.table);
      for (const id of target.ids) {
        if (table.has(this.key(id))) continue;
        let set = uncachedByTable.get(target.table);
        if (!set) {
          set = new Set();
          uncachedByTable.set(target.table, set);
        }
        set.add(id);
      }
    }

    if (uncachedByTable.size === 0) return;

    await Promise.all(
      [...uncachedByTable.entries()].map(async ([tableName, idSet]) => {
        const ids = [...idSet];
        const rows = await this.knex
          .table(tableName)
          .select('*')
          .whereIn('id', ids)
          .andWhere('_indexer', this.indexerName)
          .andWhereRaw('upper_inf(block_range)');

        const table = this.getTableMap(tableName);
        const foundKeys = new Set<string>();
        for (const row of rows) {
          const key = this.key(row.id);
          foundKeys.add(key);
          table.set(key, {
            loaded: row,
            hydrated: true,
            dirty: null,
            deleted: false,
            firstChangeBlock: null
          });
        }

        for (const id of ids) {
          const key = this.key(id);
          if (foundKeys.has(key)) continue;
          table.set(key, {
            loaded: null,
            hydrated: true,
            dirty: null,
            deleted: false,
            firstChangeBlock: null
          });
        }
      })
    );
  }

  isEmpty(): boolean {
    if (this.checkpoints.length > 0) return false;
    if (this.blockHashes.size > 0) return false;
    for (const table of this.cache.values()) {
      for (const state of table.values()) {
        if (state.dirty !== null || state.deleted) return false;
      }
    }
    return true;
  }

  clear() {
    this.cache.clear();
    this.checkpoints = [];
    this.blockHashes.clear();
  }

  async flush(lastBlockNumber: number): Promise<void> {
    const indexer = this.indexerName;
    const knex = this.knex;

    await knex.transaction(async trx => {
      for (const [tableName, table] of this.cache.entries()) {
        const closeByBlock = new Map<number, (string | number)[]>();
        const rowsToInsert: Row[] = [];

        for (const state of table.values()) {
          if (state.firstChangeBlock === null) continue;

          const hasExistingRow = state.loaded !== null;
          const hasFinalRow = state.dirty !== null && !state.deleted;
          const boundary = state.firstChangeBlock;

          if (hasExistingRow && (state.dirty !== null || state.deleted)) {
            let bucket = closeByBlock.get(boundary);
            if (!bucket) {
              bucket = [];
              closeByBlock.set(boundary, bucket);
            }
            bucket.push(state.loaded!.id);
          }

          if (hasFinalRow) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { uid: _uid, ...values } = state.dirty!;
            rowsToInsert.push({
              ...values,
              _indexer: indexer,
              block_range: knex.raw('int8range(?, NULL)', [boundary])
            });
          }
        }

        for (const [boundary, ids] of closeByBlock.entries()) {
          for (const idsChunk of chunk(ids, UPDATE_CHUNK_SIZE)) {
            await trx
              .table(tableName)
              .whereIn('id', idsChunk)
              .andWhere('_indexer', indexer)
              .andWhereRaw('upper_inf(block_range)')
              .update({
                block_range: knex.raw('int8range(lower(block_range), ?)', [
                  boundary
                ])
              });
          }
        }

        for (const rowsChunk of chunk(rowsToInsert, INSERT_CHUNK_SIZE)) {
          await trx.table(tableName).insert(rowsChunk);
        }
      }

      if (this.checkpoints.length > 0) {
        await this.store.insertCheckpoints(indexer, this.checkpoints, trx);
      }

      for (const [blockNumber, hash] of this.blockHashes.entries()) {
        await this.store.setBlockHash(indexer, blockNumber, hash, trx);
      }

      await this.store.setMetadata(
        indexer,
        MetadataId.LastIndexedBlock,
        lastBlockNumber,
        trx
      );
    });

    this.clear();
  }
}
