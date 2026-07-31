import { Knex } from 'knex';
import { EntityBuffer } from './orm/entity-buffer';

function createRegister() {
  let knexInstance: Knex | null = null;
  const currentBlocks = new Map<string, bigint>();
  const entityBuffers = new Map<string, EntityBuffer>();

  const getKnex = () => {
    if (!knexInstance) {
      throw new Error('Knex is not initialized yet.');
    }

    return knexInstance;
  };

  return {
    getCurrentBlock(indexerName: string) {
      return currentBlocks.get(indexerName) || 0n;
    },
    setCurrentBlock(indexerName: string, block: bigint) {
      currentBlocks.set(indexerName, block);
    },
    getEntityBuffer(indexerName: string) {
      let buffer = entityBuffers.get(indexerName);
      if (!buffer) {
        buffer = new EntityBuffer({ indexerName, getKnex });
        entityBuffers.set(indexerName, buffer);
      }

      return buffer;
    },
    getKnex,
    setKnex(knex: Knex) {
      knexInstance = knex;
    }
  };
}

export const register = createRegister();
