import { mockDeep } from 'jest-mock-extended';
import knex from 'knex';
import {
  CheckpointsStore,
  MetadataId,
  Table
} from '../../../src/stores/checkpoints';
import { Logger } from '../../../src/utils/logger';

function createMockLogger() {
  return mockDeep<Logger>({
    child: () => createMockLogger()
  });
}

describe('CheckpointsStore', () => {
  const INDEXER = 'default';

  const mockKnex = knex({
    client: 'sqlite3',
    connection: {
      filename: ':memory:'
    },
    useNullAsDefault: true
  });

  const logger = createMockLogger();
  const store = new CheckpointsStore(mockKnex, logger);

  afterAll(async () => {
    await mockKnex.destroy();
  });

  describe('createStore', () => {
    it('should execute correct query', async () => {
      const { builder } = await store.createStore();

      expect(builder.toString()).toMatchSnapshot();
    });
  });

  describe('removeFutureData', () => {
    afterAll(async () => {
      await store.resetStore();
    });

    it('should remove future data', async () => {
      await store.setMetadata(INDEXER, MetadataId.LastIndexedBlock, 11001);
      await store.setMetadata('OTHER', MetadataId.LastIndexedBlock, 11001);

      await store.insertCheckpoints(INDEXER, [
        {
          contractAddress: '0x01',
          blockNumber: 5000
        },
        {
          contractAddress: '0x02',
          blockNumber: 9000
        },
        {
          contractAddress: '0x01',
          blockNumber: 11000
        }
      ]);
      await store.insertCheckpoints('OTHER', [
        {
          contractAddress: '0x01',
          blockNumber: 11000
        }
      ]);

      await store.removeFutureData(INDEXER, 10000);

      const defaultLastIndexedBlock = await store.getMetadataNumber(
        INDEXER,
        MetadataId.LastIndexedBlock
      );
      expect(defaultLastIndexedBlock).toEqual(10000);

      const otherLastIndexedBlock = await store.getMetadataNumber(
        'OTHER',
        MetadataId.LastIndexedBlock
      );
      expect(otherLastIndexedBlock).toEqual(11001);

      const result = await mockKnex.select('*').from(Table.Checkpoints);
      expect(result).toMatchSnapshot();
    });
  });

  describe('blocks', () => {
    afterAll(async () => {
      await store.resetStore();
    });

    it('should set block hash', async () => {
      await store.setBlockHash(INDEXER, 5000, '0x0');
      await store.setBlockHash(INDEXER, 5001, '0x1');
      await store.setBlockHash('OTHER', 5000, '0xa');

      const result = await mockKnex.select('*').from(Table.Blocks);
      expect(result).toMatchSnapshot();
    });

    it('should retrieve block hash', async () => {
      const result = await store.getBlockHash(INDEXER, 5000);
      expect(result).toEqual('0x0');
    });

    it('should return null if retrieving non-existent block hash', async () => {
      const result = await store.getBlockHash(INDEXER, 6000);
      expect(result).toBeNull();
    });

    it('should remove blocks', async () => {
      await store.removeBlocks('OTHER');

      const result = await mockKnex.select('*').from(Table.Blocks);
      expect(result).toMatchSnapshot();
    });
  });

  describe('metadata', () => {
    afterAll(async () => {
      await store.resetStore();
    });

    it('should set metadata', async () => {
      await store.setMetadata(INDEXER, 'key', 'default_value');
      await store.setMetadata(INDEXER, 'number_key', 1111);
      await store.setMetadata('OTHER', 'key', 'other_value');

      const result = await mockKnex.select('*').from(Table.Metadata);
      expect(result).toMatchSnapshot();
    });

    it('should retrieve metadata', async () => {
      const result = await store.getMetadata(INDEXER, 'key');
      expect(result).toEqual('default_value');
    });

    it('should retrieve metadata as number', async () => {
      const result = await store.getMetadataNumber(INDEXER, 'number_key');
      expect(result).toEqual(1111);
    });

    it('should return null if retrieving non-existent metadata value', async () => {
      const result = await store.getMetadata(INDEXER, 'non_existent_key');
      expect(result).toBeNull();
    });

    it('should return null if retrieving non-existent metadata value as number', async () => {
      const result = await store.getMetadataNumber(INDEXER, 'non_existent_key');
      expect(result).toBeNull();
    });

    it('should update metadata', async () => {
      await store.setMetadata(INDEXER, 'key', 'new_value');
      const result = await store.getMetadata(INDEXER, 'key');
      expect(result).toEqual('new_value');
    });
  });

  describe('checkpoints', () => {
    const CONTRACT_A = '0x01';
    const CONTRACT_B = '0x02';

    afterAll(async () => {
      await store.resetStore();
    });

    it('should insert checkpoints', async () => {
      const checkpoints = [
        {
          contractAddress: CONTRACT_A,
          blockNumber: 5000
        },
        {
          contractAddress: CONTRACT_B,
          blockNumber: 9000
        },
        {
          contractAddress: CONTRACT_A,
          blockNumber: 11000
        }
      ];

      await store.insertCheckpoints(INDEXER, checkpoints);

      const result = await mockKnex.select('*').from(Table.Checkpoints);
      expect(result).toMatchSnapshot();
    });

    it('should return next checkpoint blocks', async () => {
      let result = await store.getNextCheckpointBlocks(INDEXER, 4000, [
        CONTRACT_A,
        CONTRACT_B
      ]);
      expect(result).toEqual([5000, 9000, 11000]);

      result = await store.getNextCheckpointBlocks(INDEXER, 7000, [
        CONTRACT_A,
        CONTRACT_B
      ]);
      expect(result).toEqual([9000, 11000]);

      result = await store.getNextCheckpointBlocks(INDEXER, 4000, [CONTRACT_B]);
      expect(result).toEqual([9000]);
    });
  });

  describe('template sources', () => {
    const CONTRACT_A = '0x01';
    const CONTRACT_B = '0x02';

    afterAll(async () => {
      await store.resetStore();
    });

    it('should insert template sources', async () => {
      await store.insertTemplateSource(INDEXER, CONTRACT_A, 1000, 'Template1');
      await store.insertTemplateSource(INDEXER, CONTRACT_A, 2000, 'Template1');
      await store.insertTemplateSource(INDEXER, CONTRACT_B, 2100, 'Template3');
      await store.insertTemplateSource('OTHER', CONTRACT_A, 50, 'Template1');

      const result = await mockKnex.select('*').from(Table.TemplateSources);
      expect(result).toMatchSnapshot();
    });

    it('should retrieve template sources', async () => {
      const result = await store.getTemplateSources(INDEXER);
      expect(result).toMatchSnapshot();
    });
  });
});
