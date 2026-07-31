import { describe, expect, it } from 'bun:test';
import knex, { Knex } from 'knex';
import { EntityBuffer } from '../../../src/orm/entity-buffer';

const pg = knex({ client: 'pg' });

/**
 * Minimal stand-in for the read-through SELECT: resolves rows by
 * `${tableName}:${id}` from a mutable map.
 */
function fakeDb(rows: Record<string, Record<string, any>> = {}): Knex {
  const db = {
    table(tableName: string) {
      let id: unknown;
      const chain = {
        select: () => chain,
        where: (_column: string, value: unknown) => {
          id = value;
          return chain;
        },
        andWhere: () => chain,
        andWhereRaw: () => chain,
        first: async () => rows[`${tableName}:${id}`]
      };
      return chain;
    }
  };

  return db as unknown as Knex;
}

function createBuffer(db: Knex = pg) {
  return new EntityBuffer({ indexerName: 'eth', getKnex: () => db });
}

describe('EntityBuffer', () => {
  describe('load', () => {
    it('loads unknown entities from the database and caches them', async () => {
      const rows = { 'delegates:0x1': { id: '0x1', votes: 1 } };
      const buffer = createBuffer(fakeDb(rows));

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 1
      });

      delete rows['delegates:0x1'];
      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 1
      });
      expect(buffer.size).toBe(1);
    });

    it('counts accumulated version history in size', () => {
      const buffer = createBuffer();

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 105n,
        hasDatabaseRow: false
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 3 },
        block: 110n,
        hasDatabaseRow: false
      });

      expect(buffer.size).toBe(3);
    });

    it('returns null for entities missing in the database', async () => {
      const buffer = createBuffer(fakeDb());

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toBe(
        null
      );
    });

    it('returns saved values without touching the database', async () => {
      const buffer = createBuffer(
        fakeDb({ 'delegates:0x1': { id: '0x1', votes: 999 } })
      );

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 100n,
        hasDatabaseRow: false
      });

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 2
      });
    });

    it('clones values on save and load', async () => {
      const buffer = createBuffer(fakeDb());

      const values = { id: '0x1', votes: 1 };
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values,
        block: 100n,
        hasDatabaseRow: false
      });
      values.votes = 42;

      const result = await buffer.load({ tableName: 'delegates', id: '0x1' });
      if (!result) throw new Error('expected values');
      expect(result.votes).toBe(1);

      result.votes = 43;
      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 1
      });
    });
  });

  describe('version tracking', () => {
    it('collapses same-block re-saves into a single version', () => {
      const buffer = createBuffer();

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 100n,
        hasDatabaseRow: false
      });

      expect(buffer.size).toBe(1);

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe(
        `insert into "delegates" ("_indexer", "block_range", "id", "votes") values ('eth', int8range('100', NULL), '0x1', 2)`
      );
    });

    it('keeps intermediate versions for saves at different blocks', () => {
      const buffer = createBuffer();

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 105n,
        hasDatabaseRow: false
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe(
        `insert into "delegates" ("_indexer", "block_range", "id", "votes") values ('eth', int8range('100', '105'), '0x1', 1), ('eth', int8range('105', NULL), '0x1', 2)`
      );
    });

    it('closes the database row once when a loaded entity is updated', async () => {
      const buffer = createBuffer(
        fakeDb({ 'delegates:0x1': { id: '0x1', votes: 1 } })
      );

      await buffer.load({ tableName: 'delegates', id: '0x1' });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 100n,
        hasDatabaseRow: true
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 3 },
        block: 105n,
        hasDatabaseRow: true
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe(
        `update "delegates" set "block_range" = int8range(lower(block_range), '100') where "id" in ('0x1') and "_indexer" = 'eth' and upper_inf(block_range)`
      );
      expect(statements[1]).toContain('insert into "delegates"');
    });

    it('closes the database row for entities known to exist without a prior read', () => {
      const buffer = createBuffer();

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 100n,
        hasDatabaseRow: true
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain(`int8range(lower(block_range), '100')`);
      expect(statements[1]).toContain('insert into "delegates"');
    });

    it('does not emit a close for entities created in the buffer', () => {
      const buffer = createBuffer();

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toStartWith('insert into "delegates"');
    });
  });

  describe('delete', () => {
    it('closes the database row and shields the stale row from reads', async () => {
      const rows = { 'delegates:0x1': { id: '0x1', votes: 1 } };
      const buffer = createBuffer(fakeDb(rows));

      await buffer.load({ tableName: 'delegates', id: '0x1' });
      buffer.delete({
        tableName: 'delegates',
        id: '0x1',
        block: 100n,
        hasDatabaseRow: true
      });

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toBe(
        null
      );

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toStartWith('update "delegates"');
    });

    it('leaves nothing pending for entities created and deleted in the same block', async () => {
      const rows = { 'delegates:0x1': { id: '0x1', votes: 1 } };
      const buffer = createBuffer(fakeDb(rows));

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.delete({
        tableName: 'delegates',
        id: '0x1',
        block: 100n,
        hasDatabaseRow: false
      });

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toBe(
        null
      );
      expect(buffer.prepareFlush(pg)).toHaveLength(0);
    });

    it('supports re-creating a deleted entity', async () => {
      const buffer = createBuffer(
        fakeDb({ 'delegates:0x1': { id: '0x1', votes: 1 } })
      );

      await buffer.load({ tableName: 'delegates', id: '0x1' });
      buffer.delete({
        tableName: 'delegates',
        id: '0x1',
        block: 100n,
        hasDatabaseRow: true
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 5 },
        block: 105n,
        hasDatabaseRow: true
      });

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 5
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain(`int8range(lower(block_range), '100')`);
      expect(statements[1]).toContain(`int8range('105', NULL)`);
    });
  });

  describe('block journal', () => {
    it('rolls back entity changes and bookkeeping from the current block', async () => {
      const buffer = createBuffer(
        fakeDb({ 'delegates:0x1': { id: '0x1', votes: 1 } })
      );

      await buffer.load({ tableName: 'delegates', id: '0x1' });
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 100n,
        hasDatabaseRow: true
      });
      buffer.setLastIndexedBlock(100);

      buffer.beginBlock();
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 3 },
        block: 105n,
        hasDatabaseRow: true
      });
      buffer.save({
        tableName: 'delegates',
        id: '0x2',
        values: { id: '0x2', votes: 1 },
        block: 105n,
        hasDatabaseRow: false
      });
      buffer.addCheckpoints([{ blockNumber: 105, contractAddress: '0xc' }]);
      buffer.setBlockHash(105, '0xhash');
      buffer.setLastIndexedBlock(105);
      buffer.rollbackBlock();

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 2
      });
      expect(await buffer.load({ tableName: 'delegates', id: '0x2' })).toBe(
        null
      );
      expect(buffer.getBlockHash(105)).toBe(null);
      // entity close + entity insert + restored lastIndexedBlock upsert
      expect(buffer.prepareFlush(pg)).toHaveLength(3);
    });

    it('rolls back closed versions appended in the current block', async () => {
      const buffer = createBuffer(fakeDb());

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });

      buffer.beginBlock();
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 2 },
        block: 105n,
        hasDatabaseRow: false
      });
      buffer.rollbackBlock();

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 1
      });

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe(
        `insert into "delegates" ("_indexer", "block_range", "id", "votes") values ('eth', int8range('100', NULL), '0x1', 1)`
      );
    });

    it('keeps changes after commitBlock', async () => {
      const buffer = createBuffer(fakeDb());

      buffer.beginBlock();
      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.commitBlock();
      buffer.rollbackBlock();

      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toEqual({
        id: '0x1',
        votes: 1
      });
    });
  });

  describe('bookkeeping', () => {
    it('builds checkpoint, block hash and metadata statements', () => {
      const buffer = createBuffer();

      buffer.addCheckpoints([
        { blockNumber: 100, contractAddress: '0xc' },
        { blockNumber: 105, contractAddress: '0xc' }
      ]);
      buffer.setBlockHash(105, '0xhash');
      buffer.setLastIndexedBlock(105);

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(3);
      expect(statements[0]).toContain('insert into "_checkpoints"');
      expect(statements[0]).toContain(
        'on conflict ("id", "indexer") do nothing'
      );
      expect(statements[1]).toContain('insert into "_blocks"');
      expect(statements[1]).toContain(
        'on conflict ("indexer", "block_number") do update'
      );
      expect(statements[2]).toContain('insert into "_metadatas"');
      expect(statements[2]).toContain(
        'on conflict ("id", "indexer") do update'
      );
    });

    it('keeps only the latest hash per block number', () => {
      const buffer = createBuffer();

      buffer.setBlockHash(100, '0xold');
      buffer.setBlockHash(100, '0xnew');

      const statements = buffer.prepareFlush(pg).map(s => s.toString());
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain('0xnew');
      expect(statements[0]).not.toContain('0xold');
    });

    it('returns the latest buffered block hash', () => {
      const buffer = createBuffer();

      buffer.setBlockHash(100, '0xaaa');
      buffer.setBlockHash(105, '0xbbb');

      expect(buffer.getBlockHash(100)).toBe('0xaaa');
      expect(buffer.getBlockHash(105)).toBe('0xbbb');
      expect(buffer.getBlockHash(110)).toBe(null);
    });
  });

  describe('reset', () => {
    it('clears all pending state', async () => {
      const buffer = createBuffer(fakeDb());

      buffer.save({
        tableName: 'delegates',
        id: '0x1',
        values: { id: '0x1', votes: 1 },
        block: 100n,
        hasDatabaseRow: false
      });
      buffer.addCheckpoints([{ blockNumber: 100, contractAddress: '0xc' }]);
      buffer.setBlockHash(100, '0xhash');
      buffer.setLastIndexedBlock(100);
      buffer.reset();

      expect(buffer.size).toBe(0);
      expect(await buffer.load({ tableName: 'delegates', id: '0x1' })).toBe(
        null
      );
      expect(buffer.getBlockHash(100)).toBe(null);
      expect(buffer.prepareFlush(pg)).toHaveLength(0);
    });
  });
});
