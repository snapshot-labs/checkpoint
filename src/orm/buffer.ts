import { Knex } from 'knex';
import {
  CheckpointRecord,
  CheckpointsStore,
  MetadataId
} from '../stores/checkpoints';
import { chunk } from '../utils/helpers';

type Row = Record<string, any>;

type EntityState = {
  loaded: Row | null;
  hydrated: boolean;
  dirty: Row | null;
  deleted: boolean;
};

const UPDATE_CHUNK_SIZE = 1000;
const INSERT_CHUNK_SIZE = 1000;

export class EntityWriteBuffer {
  private readonly indexerName: string;
  private readonly knex: Knex;
  private readonly store: CheckpointsStore;

  private cache = new Map<string, Map<string, EntityState>>();
  private checkpoints: CheckpointRecord[] = [];

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
      deleted: false
    });

    return loaded;
  }

  stageInsert(tableName: string, id: string | number, row: Row) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.dirty = row;
      existing.deleted = false;
      return;
    }

    table.set(key, {
      loaded: null,
      hydrated: true,
      dirty: row,
      deleted: false
    });
  }

  stageUpdate(tableName: string, id: string | number, row: Row) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.dirty = row;
      existing.deleted = false;
      return;
    }

    // Save called on a model that was never loaded via the buffer —
    // treat as insert of a pre-existing row. Flush still needs to close
    // the previous range, so mark it as hydrated with unknown loaded state
    // by forcing a DB lookup at flush time is expensive; instead we trust
    // the caller's `exists` flag and emit both the range-close and insert.
    table.set(key, {
      loaded: { id } as Row,
      hydrated: true,
      dirty: row,
      deleted: false
    });
  }

  stageDelete(tableName: string, id: string | number) {
    const table = this.getTableMap(tableName);
    const key = this.key(id);
    const existing = table.get(key);

    if (existing) {
      existing.deleted = true;
      return;
    }

    table.set(key, {
      loaded: { id } as Row,
      hydrated: true,
      dirty: null,
      deleted: true
    });
  }

  pushCheckpoints(records: CheckpointRecord[]) {
    if (records.length === 0) return;
    this.checkpoints.push(...records);
  }

  isEmpty(): boolean {
    if (this.checkpoints.length > 0) return false;
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
  }

  async flush({
    blockNumber,
    blockHash
  }: {
    blockNumber: number;
    blockHash: string | null;
  }): Promise<void> {
    const indexer = this.indexerName;
    const knex = this.knex;

    await knex.transaction(async trx => {
      for (const [tableName, table] of this.cache.entries()) {
        const idsToCloseRange: (string | number)[] = [];
        const rowsToInsert: Row[] = [];

        for (const [, state] of table.entries()) {
          const hasExistingRow = state.loaded !== null;
          const hasFinalRow = state.dirty !== null && !state.deleted;

          if (hasExistingRow && (state.dirty !== null || state.deleted)) {
            idsToCloseRange.push(state.loaded!.id);
          }

          if (hasFinalRow) {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { uid: _uid, ...values } = state.dirty!;
            rowsToInsert.push({
              ...values,
              _indexer: indexer,
              block_range: knex.raw('int8range(?, NULL)', [blockNumber])
            });
          }
        }

        for (const idsChunk of chunk(idsToCloseRange, UPDATE_CHUNK_SIZE)) {
          await trx
            .table(tableName)
            .whereIn('id', idsChunk)
            .andWhere('_indexer', indexer)
            .andWhereRaw('upper_inf(block_range)')
            .update({
              block_range: knex.raw('int8range(lower(block_range), ?)', [
                blockNumber
              ])
            });
        }

        for (const rowsChunk of chunk(rowsToInsert, INSERT_CHUNK_SIZE)) {
          await trx.table(tableName).insert(rowsChunk);
        }
      }

      if (this.checkpoints.length > 0) {
        await this.store.insertCheckpoints(indexer, this.checkpoints, trx);
      }

      if (blockHash !== null) {
        await this.store.setBlockHash(indexer, blockNumber, blockHash, trx);
      }

      await this.store.setMetadata(
        indexer,
        MetadataId.LastIndexedBlock,
        blockNumber,
        trx
      );
    });

    this.clear();
  }
}
