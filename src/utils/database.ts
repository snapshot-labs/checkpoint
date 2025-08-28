import { Knex } from 'knex';
import pluralize from 'pluralize';
import { INTERNAL_TABLES } from '../stores/checkpoints';

export type QueryFilter = {
  block?: number;
  indexer?: string;
};

export const getTableName = (name: string) => {
  if (name === '_metadata') return '_metadatas';

  return pluralize(name);
};

export function applyQueryFilter(
  query: Knex.QueryBuilder,
  tableName: string,
  filters: QueryFilter
) {
  const isInternalTable = INTERNAL_TABLES.includes(tableName);

  let filteredQuery = query;

  if (!isInternalTable) {
    filteredQuery =
      filters.block !== undefined
        ? query.andWhereRaw(`${tableName}.block_range @> int8(??)`, [
            filters.block
          ])
        : query.andWhereRaw(`upper_inf(${tableName}.block_range)`);
  }

  if (filters.indexer !== undefined) {
    const columnName = isInternalTable ? 'indexer' : `_indexer`;

    filteredQuery = query.andWhere(
      `${tableName}.${columnName}`,
      filters.indexer
    );
  }

  return filteredQuery;
}

/**
 * Applies the default order to the query.
 * All entities are by default sorted by block_range in ascending order.
 * This function is used to ensure that the order is consistent across all queries.
 * @param query Knex query builder
 * @param tableName The name of the table to apply the order on
 * @returns The modified query with the default order applied
 */
export function applyDefaultOrder(query: Knex.QueryBuilder, tableName: string) {
  const isInternalTable = INTERNAL_TABLES.includes(tableName);

  if (isInternalTable) return query;

  return query.orderBy(`${tableName}.block_range`);
}
