import { Knex } from 'knex';
import {
  CheckpointRecord,
  Fields,
  getCheckpointId,
  MetadataId,
  Table
} from '../stores/checkpoints';
import { chunk } from '../utils/helpers';

type EntityValues = Record<string, any>;

type PendingVersion = {
  lower: bigint;
  upper: bigint | null;
  values: EntityValues;
};

type BufferedEntity = {
  tableName: string;
  id: string;
  databaseRowValues: EntityValues | null;
  hasDatabaseRow: boolean;
  closeDatabaseRowAt: bigint | null;
  closedVersions: PendingVersion[];
  openVersion: PendingVersion | null;
  deleted: boolean;
};

type JournalSnapshot = {
  closeDatabaseRowAt: bigint | null;
  openVersion: PendingVersion | null;
  deleted: boolean;
  closedVersionsLength: number;
};

type BlockHashRecord = { blockNumber: number; hash: string };

const MAX_INSERT_BINDINGS = 60000;
const MAX_INSERT_ROWS = 1000;

function cloneVersion(version: PendingVersion): PendingVersion {
  return { ...version, values: { ...version.values } };
}

function snapshotEntity(entity: BufferedEntity): JournalSnapshot {
  return {
    closeDatabaseRowAt: entity.closeDatabaseRowAt,
    openVersion: entity.openVersion ? cloneVersion(entity.openVersion) : null,
    deleted: entity.deleted,
    closedVersionsLength: entity.closedVersions.length
  };
}

type Journal = {
  entities: Map<string, JournalSnapshot | null>;
  checkpointCount: number;
  blockHashCount: number;
  lastIndexedBlock: number | null;
};

/**
 * Write-behind buffer for entity writes of a single indexer.
 *
 * `Model` routes all reads and writes through this buffer, `Container`
 * flushes it. A flush persists all pending version rows and bookkeeping
 * (checkpoints, block hashes, last indexed block) in a single transaction,
 * making indexing atomic per flush window and crash replay safe.
 */
export class EntityBuffer {
  private readonly indexerName: string;
  private readonly getKnex: () => Knex;

  private entities = new Map<string, BufferedEntity>();
  private checkpoints: CheckpointRecord[] = [];
  private blockHashes: BlockHashRecord[] = [];
  private lastIndexedBlock: number | null = null;

  private journal: Journal | null = null;

  constructor({
    indexerName,
    getKnex
  }: {
    indexerName: string;
    getKnex: () => Knex;
  }) {
    this.indexerName = indexerName;
    this.getKnex = getKnex;
  }

  get size(): number {
    let count =
      this.entities.size + this.checkpoints.length + this.blockHashes.length;
    for (const entity of this.entities.values()) {
      count += entity.closedVersions.length;
    }

    return count;
  }

  async load({
    tableName,
    id
  }: {
    tableName: string;
    id: string;
  }): Promise<EntityValues | null> {
    const key = this.getKey(tableName, id);
    const entity = this.entities.get(key);
    if (entity) {
      if (entity.openVersion) return { ...entity.openVersion.values };
      if (entity.deleted) return null;
      if (entity.databaseRowValues) return { ...entity.databaseRowValues };
    }

    const row = await this.getKnex()
      .table(tableName)
      .select('*')
      .where('id', id)
      .andWhere('_indexer', this.indexerName)
      .andWhereRaw('upper_inf(block_range)')
      .first();
    if (!row) return null;

    this.entities.set(key, {
      tableName,
      id,
      databaseRowValues: { ...row },
      hasDatabaseRow: true,
      closeDatabaseRowAt: null,
      closedVersions: [],
      openVersion: null,
      deleted: false
    });

    return row;
  }

  save({
    tableName,
    id,
    values,
    block,
    hasDatabaseRow
  }: {
    tableName: string;
    id: string;
    values: EntityValues;
    block: bigint;
    hasDatabaseRow: boolean;
  }) {
    const entity = this.touch(tableName, id, hasDatabaseRow);

    if (entity.openVersion && entity.openVersion.lower === block) {
      entity.openVersion.values = { ...values };
    } else {
      this.closeCurrent(entity, block);
      entity.openVersion = { lower: block, upper: null, values: { ...values } };
    }

    entity.deleted = false;
  }

  delete({
    tableName,
    id,
    block,
    hasDatabaseRow
  }: {
    tableName: string;
    id: string;
    block: bigint;
    hasDatabaseRow: boolean;
  }) {
    const entity = this.touch(tableName, id, hasDatabaseRow);

    this.closeCurrent(entity, block);
    entity.deleted = true;
  }

  private closeCurrent(entity: BufferedEntity, block: bigint) {
    if (entity.openVersion) {
      if (entity.openVersion.lower !== block) {
        entity.closedVersions.push({ ...entity.openVersion, upper: block });
      }

      entity.openVersion = null;
    } else if (entity.hasDatabaseRow && entity.closeDatabaseRowAt === null) {
      entity.closeDatabaseRowAt = block;
    }
  }

  addCheckpoints(records: CheckpointRecord[]) {
    this.checkpoints.push(...records);
  }

  setBlockHash(blockNumber: number, hash: string) {
    this.blockHashes.push({ blockNumber, hash });
  }

  getBlockHash(blockNumber: number): string | null {
    for (let i = this.blockHashes.length - 1; i >= 0; i--) {
      if (this.blockHashes[i].blockNumber === blockNumber) {
        return this.blockHashes[i].hash;
      }
    }

    return null;
  }

  setLastIndexedBlock(block: number) {
    this.lastIndexedBlock = block;
  }

  beginBlock() {
    this.journal = {
      entities: new Map(),
      checkpointCount: this.checkpoints.length,
      blockHashCount: this.blockHashes.length,
      lastIndexedBlock: this.lastIndexedBlock
    };
  }

  commitBlock() {
    this.journal = null;
  }

  rollbackBlock() {
    if (!this.journal) return;

    for (const [key, snapshot] of this.journal.entities) {
      if (snapshot === null) {
        this.entities.delete(key);
        continue;
      }

      const entity = this.entities.get(key);
      if (!entity) continue;

      entity.closeDatabaseRowAt = snapshot.closeDatabaseRowAt;
      entity.openVersion = snapshot.openVersion;
      entity.deleted = snapshot.deleted;
      entity.closedVersions.length = snapshot.closedVersionsLength;
    }

    this.checkpoints.length = this.journal.checkpointCount;
    this.blockHashes.length = this.journal.blockHashCount;
    this.lastIndexedBlock = this.journal.lastIndexedBlock;

    this.commitBlock();
  }

  prepareFlush(knex: Knex): Knex.QueryBuilder[] {
    const statements: Knex.QueryBuilder[] = [];

    const closes = new Map<
      string,
      { tableName: string; closeAt: bigint; ids: string[] }
    >();
    const insertsByTable = new Map<string, PendingVersion[]>();

    for (const entity of this.entities.values()) {
      if (entity.closeDatabaseRowAt !== null) {
        const key = `${entity.tableName} ${entity.closeDatabaseRowAt}`;
        const group = closes.get(key) ?? {
          tableName: entity.tableName,
          closeAt: entity.closeDatabaseRowAt,
          ids: []
        };
        group.ids.push(entity.id);
        closes.set(key, group);
      }

      const versions = [
        ...entity.closedVersions,
        ...(entity.openVersion ? [entity.openVersion] : [])
      ];
      if (versions.length > 0) {
        const inserts = insertsByTable.get(entity.tableName) ?? [];
        inserts.push(...versions);
        insertsByTable.set(entity.tableName, inserts);
      }
    }

    for (const { tableName, closeAt, ids } of closes.values()) {
      statements.push(
        knex
          .table(tableName)
          .whereIn('id', ids)
          .andWhere('_indexer', this.indexerName)
          .andWhereRaw('upper_inf(block_range)')
          .update({
            block_range: knex.raw('int8range(lower(block_range), ?)', [
              closeAt.toString()
            ])
          })
      );
    }

    for (const [tableName, versions] of insertsByTable) {
      const rows = versions.map(version => ({
        ...version.values,
        _indexer: this.indexerName,
        block_range:
          version.upper === null
            ? knex.raw('int8range(?, NULL)', [version.lower.toString()])
            : knex.raw('int8range(?, ?)', [
                version.lower.toString(),
                version.upper.toString()
              ])
      }));

      const columnCount = Object.keys(rows[0]).length + 1;
      const chunkSize = Math.min(
        MAX_INSERT_ROWS,
        Math.floor(MAX_INSERT_BINDINGS / columnCount)
      );
      for (const rowsChunk of chunk(rows, chunkSize)) {
        statements.push(knex.table(tableName).insert(rowsChunk));
      }
    }

    if (this.checkpoints.length > 0) {
      for (const checkpointsChunk of chunk(this.checkpoints, MAX_INSERT_ROWS)) {
        statements.push(
          knex
            .table(Table.Checkpoints)
            .insert(
              checkpointsChunk.map(checkpoint => ({
                [Fields.Checkpoints.Id]: getCheckpointId(
                  checkpoint.contractAddress,
                  checkpoint.blockNumber
                ),
                [Fields.Checkpoints.Indexer]: this.indexerName,
                [Fields.Checkpoints.BlockNumber]: checkpoint.blockNumber,
                [Fields.Checkpoints.ContractAddress]: checkpoint.contractAddress
              }))
            )
            .onConflict([Fields.Checkpoints.Id, Fields.Checkpoints.Indexer])
            .ignore()
        );
      }
    }

    if (this.blockHashes.length > 0) {
      const latestHashes = new Map<number, string>();
      for (const record of this.blockHashes) {
        latestHashes.set(record.blockNumber, record.hash);
      }

      statements.push(
        knex
          .table(Table.Blocks)
          .insert(
            [...latestHashes.entries()].map(([blockNumber, hash]) => ({
              [Fields.Blocks.Indexer]: this.indexerName,
              [Fields.Blocks.Number]: blockNumber,
              [Fields.Blocks.Hash]: hash
            }))
          )
          .onConflict([Fields.Blocks.Indexer, Fields.Blocks.Number])
          .merge()
      );
    }

    if (this.lastIndexedBlock !== null) {
      statements.push(
        knex
          .table(Table.Metadata)
          .insert({
            [Fields.Metadata.Id]: MetadataId.LastIndexedBlock,
            [Fields.Metadata.Indexer]: this.indexerName,
            [Fields.Metadata.Value]: this.lastIndexedBlock
          })
          .onConflict([Fields.Metadata.Id, Fields.Metadata.Indexer])
          .merge()
      );
    }

    return statements;
  }

  async flush() {
    const knex = this.getKnex();
    const statements = this.prepareFlush(knex);

    if (statements.length === 0) {
      this.reset();
      return;
    }

    await knex.transaction(async trx => {
      for (const statement of statements) {
        await statement.transacting(trx);
      }
    });

    this.reset();
  }

  reset() {
    this.entities.clear();
    this.checkpoints = [];
    this.blockHashes = [];
    this.lastIndexedBlock = null;
    this.commitBlock();
  }

  private getKey(tableName: string, id: string): string {
    return `${tableName} ${id}`;
  }

  private touch(
    tableName: string,
    id: string,
    hasDatabaseRow: boolean
  ): BufferedEntity {
    const key = this.getKey(tableName, id);
    let entity = this.entities.get(key);

    if (this.journal && !this.journal.entities.has(key)) {
      this.journal.entities.set(key, entity ? snapshotEntity(entity) : null);
    }

    if (!entity) {
      entity = {
        tableName,
        id,
        databaseRowValues: null,
        hasDatabaseRow,
        closeDatabaseRowAt: null,
        closedVersions: [],
        openVersion: null,
        deleted: false
      };
      this.entities.set(key, entity);
    }

    return entity;
  }
}
