import DataLoader from 'dataloader';
import {
  GraphQLField,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLResolveInfo,
  GraphQLScalarType,
  isListType,
  isObjectType,
  isScalarType
} from 'graphql';
import {
  parseResolveInfo,
  simplifyParsedResolveInfoFragmentWithType
} from 'graphql-parse-resolve-info';
import { Knex } from 'knex';
import { Pool as PgPool } from 'pg';
import {
  applyDefaultOrder,
  applyQueryFilter,
  getTableName,
  QueryFilter
} from '../utils/database';
import { getDerivedFromDirective, getNonNullType } from '../utils/graphql';
import { Logger } from '../utils/logger';

type BaseArgs = {
  block?: number;
  indexer?: string;
};

type SingleEntitySource = Record<string, any> & {
  _args: BaseArgs;
};

type Result = Record<string, any> & {
  _args?: BaseArgs;
};

type SingleEntityResolverArgs = BaseArgs & {
  id: string;
};

type MultiEntityResolverArgs = BaseArgs & {
  first?: number;
  skip?: number;
  orderBy?: string;
  orderDirection?: string;
  where?: Record<string, any>;
};

export type ResolverContextInput = {
  log: Logger;
  knex: Knex;
  pg: PgPool;
};

export type ResolverContext = ResolverContextInput & {
  getLoader: (
    name: string,
    field: string,
    queryFilter: QueryFilter
  ) => DataLoader<readonly unknown[], any>;
};

export async function queryMulti(
  parent: undefined,
  args: MultiEntityResolverArgs,
  context: ResolverContext,
  info: GraphQLResolveInfo
): Promise<Result[]> {
  const { log, knex } = context;

  const nonNullType = getNonNullType(info.returnType);
  if (!isListType(nonNullType)) throw new Error('unexpected return type');
  const returnType = getNonNullType(nonNullType.ofType) as GraphQLObjectType;
  const jsonFields = getJsonFields(returnType);

  const tableName = getTableName(returnType.name.toLowerCase());

  const nestedEntitiesMappings = {} as Record<string, Record<string, string>>;

  let query = knex.select(`${tableName}.*`).from(tableName);
  query = applyQueryFilter(query, tableName, {
    block: args.block,
    indexer: args.indexer
  });

  const handleWhere = (
    query: Knex.QueryBuilder,
    prefix: string,
    where: Record<string, any>
  ) => {
    const isFieldList = (fieldName: string) => {
      const fieldType = getNonNullType(returnType.getFields()[fieldName].type);
      return isListType(fieldType);
    };

    Object.entries(where).map((w: [string, any]) => {
      // TODO: we could generate where as objects { name, column, operator, value }
      // so we don't have to cut it there

      if (w[0].endsWith('_not')) {
        const fieldName = w[0].slice(0, -4);
        const isList = isFieldList(fieldName);

        if (isList) {
          query = query.whereRaw(
            `NOT :field: @> :value::jsonb OR NOT :field: <@ :value::jsonb`,
            {
              field: `${prefix}.${fieldName}`,
              value: JSON.stringify(w[1])
            }
          );
        } else {
          query = query.where(`${prefix}.${fieldName}`, '!=', w[1]);
        }
      } else if (w[0].endsWith('_gt')) {
        query = query.where(`${prefix}.${w[0].slice(0, -3)}`, '>', w[1]);
      } else if (w[0].endsWith('_gte')) {
        query = query.where(`${prefix}.${w[0].slice(0, -4)}`, '>=', w[1]);
      } else if (w[0].endsWith('_lt')) {
        query = query.where(`${prefix}.${w[0].slice(0, -3)}`, '<', w[1]);
      } else if (w[0].endsWith('_lte')) {
        query = query.where(`${prefix}.${w[0].slice(0, -4)}`, '<=', w[1]);
      } else if (w[0].endsWith('_not_contains')) {
        const fieldName = w[0].slice(0, -13);
        const isList = isFieldList(fieldName);

        if (isList) {
          const arrayBindings = w[1].map(() => '?').join(', ');
          query = query.whereRaw(`NOT ?? \\?| array[${arrayBindings}]`, [
            `${prefix}.${fieldName}`,
            ...w[1]
          ]);
        } else {
          query = query.not.whereLike(`${prefix}.${fieldName}`, `%${w[1]}%`);
        }
      } else if (w[0].endsWith('_not_contains_nocase')) {
        query = query.not.whereILike(
          `${prefix}.${w[0].slice(0, -20)}`,
          `%${w[1]}%`
        );
      } else if (w[0].endsWith('_contains')) {
        const fieldName = w[0].slice(0, -9);
        const isList = isFieldList(fieldName);

        if (isList) {
          const arrayBindings = w[1].map(() => '?').join(', ');
          query = query.whereRaw(`?? \\?& array[${arrayBindings}]`, [
            `${prefix}.${fieldName}`,
            ...w[1]
          ]);
        } else {
          query = query.whereLike(`${prefix}.${fieldName}`, `%${w[1]}%`);
        }
      } else if (w[0].endsWith('_contains_nocase')) {
        query = query.whereILike(
          `${prefix}.${w[0].slice(0, -16)}`,
          `%${w[1]}%`
        );
      } else if (w[0].endsWith('_not_in')) {
        query = query.not.whereIn(`${prefix}.${w[0].slice(0, -7)}`, w[1]);
      } else if (w[0].endsWith('_in')) {
        query = query.whereIn(`${prefix}.${w[0].slice(0, -3)}`, w[1]);
      } else if (typeof w[1] === 'object' && w[0].endsWith('_')) {
        const fieldName = w[0].slice(0, -1);
        const nestedReturnType = getNonNullType(
          returnType.getFields()[fieldName].type as GraphQLObjectType
        );
        const nestedTableName = getTableName(
          nestedReturnType.name.toLowerCase()
        );

        const fields = Object.values(nestedReturnType.getFields())
          .filter(field => {
            const baseType = getNonNullType(field.type);

            return (
              isScalarType(baseType) ||
              isObjectType(baseType) ||
              (isListType(baseType) && !getDerivedFromDirective(field))
            );
          })
          .map(field => field.name);

        nestedEntitiesMappings[fieldName] = {
          [`${fieldName}.id`]: `${nestedTableName}.id`,
          ...Object.fromEntries(
            fields.map(field => [
              `${fieldName}.${field}`,
              `${nestedTableName}.${field}`
            ])
          )
        };

        query = query
          .columns(nestedEntitiesMappings[fieldName])
          .innerJoin(
            nestedTableName,
            `${tableName}.${fieldName}`,
            '=',
            `${nestedTableName}.id`
          )
          .whereRaw('?? = ??', [
            `${tableName}._indexer`,
            `${nestedTableName}._indexer`
          ]);

        query = applyQueryFilter(query, nestedTableName, {
          block: args.block,
          indexer: args.indexer
        });

        handleWhere(query, nestedTableName, w[1]);
      } else {
        const fieldName = w[0];
        const isList = isFieldList(fieldName);

        if (isList) {
          query = query.whereRaw(
            `:field: @> :value::jsonb AND :field: <@ :value::jsonb`,
            {
              field: `${prefix}.${fieldName}`,
              value: JSON.stringify(w[1])
            }
          );
        } else {
          query = query.where(`${prefix}.${fieldName}`, w[1]);
        }
      }
    });
  };

  if (args.where) {
    handleWhere(query, tableName, args.where);
  }

  if (args.orderBy) {
    query = query.orderBy(
      `${tableName}.${args.orderBy}`,
      args.orderDirection?.toLowerCase() || 'desc'
    );
  }

  query = applyDefaultOrder(query, tableName);

  query = query.limit(args?.first || 1000).offset(args?.skip || 0);
  log.debug({ sql: query.toQuery(), args }, 'executing multi query');

  const result = await query;
  return result.map(item => {
    const nested = Object.fromEntries(
      Object.entries(nestedEntitiesMappings).map(([fieldName, mapping]) => {
        return [
          fieldName,
          Object.fromEntries(
            Object.entries(mapping).map(([to, from]) => {
              const exploded = from.split('.');
              const key = exploded[exploded.length - 1];

              return [key, item[to]];
            })
          )
        ];
      })
    );

    return {
      ...formatItem(item, jsonFields),
      ...nested,
      _args: {
        block: args.block,
        indexer: args.indexer
      }
    };
  });
}

export async function querySingle(
  parent: SingleEntitySource | undefined,
  args: SingleEntityResolverArgs,
  context: ResolverContext,
  info: GraphQLResolveInfo
): Promise<Result | null> {
  const queryFilter = {
    block: parent?._args.block ?? args.block,
    indexer: parent?._args.indexer ?? args.indexer
  };

  const returnType = getNonNullType(info.returnType) as GraphQLObjectType;
  const jsonFields = getJsonFields(returnType);

  const parentResolvedValue = parent?.[info.fieldName];

  if (parentResolvedValue === null) return null;
  const alreadyResolvedInParent = typeof parentResolvedValue === 'object';
  if (alreadyResolvedInParent) {
    return {
      ...formatItem(parentResolvedValue, jsonFields),
      _args: queryFilter
    };
  }

  const parsed = parseResolveInfo(info);
  if (parsed && parentResolvedValue) {
    const simplified = simplifyParsedResolveInfoFragmentWithType(
      // @ts-ignore
      parsed,
      returnType
    );

    if (
      Object.keys(simplified.fields).length === 1 &&
      simplified.fields['id']
    ) {
      return { id: parentResolvedValue, _args: queryFilter };
    }
  }

  const id = parentResolvedValue || args.id;
  const items = await context
    .getLoader(returnType.name.toLowerCase(), 'id', queryFilter)
    .load(id);
  if (items.length === 0) {
    throw new Error(`Row not found: ${id}`);
  }

  return {
    ...formatItem(items[0], jsonFields),
    _args: queryFilter
  };
}

export const getNestedResolver =
  (columnName: string) =>
  async (
    parent: Result,
    args: unknown,
    context: ResolverContext,
    info: GraphQLResolveInfo
  ): Promise<Result[]> => {
    const { getLoader } = context;

    const queryFilter = {
      block: parent._args?.block,
      indexer: parent._args?.indexer
    };

    const returnType = getNonNullType(info.returnType) as
      | GraphQLList<GraphQLObjectType>
      | GraphQLList<GraphQLNonNull<GraphQLObjectType>>;
    const jsonFields = getJsonFields(
      getNonNullType(returnType.ofType) as GraphQLObjectType
    );

    const parentType = getNonNullType(info.parentType) as GraphQLObjectType;
    const field = parentType.getFields()[info.fieldName];

    const fieldType =
      info.returnType instanceof GraphQLNonNull
        ? info.returnType.ofType
        : info.returnType;
    if (!isListType(fieldType)) return [];

    const derivedFromDirective = getDerivedFromDirective(field);

    let result: Record<string, any>[] = [];
    if (!derivedFromDirective) {
      const loaderResult = await getLoader(
        columnName,
        'id',
        queryFilter
      ).loadMany(parent[info.fieldName]);

      // NOTE: loader returns array of arrays when used with loadMany, because in some cases,
      // for example when fetching derived entities we expect multiple results for a single id
      // this is why we need to flatten it. In the future it would be nice to have clearer API
      result = loaderResult.flat();
    } else {
      const fieldArgument = derivedFromDirective.arguments?.find(
        arg => arg.name.value === 'field'
      );
      if (!fieldArgument || fieldArgument.value.kind !== 'StringValue') {
        throw new Error(
          `field ${field.name} is missing field in derivedFrom directive`
        );
      }

      result = await getLoader(
        columnName,
        fieldArgument.value.value,
        queryFilter
      ).load(parent.id);
    }

    return result.map(item => ({
      ...formatItem(item, jsonFields),
      _args: queryFilter
    }));
  };

function getJsonFields(type: GraphQLObjectType) {
  return Object.values(type.getFields()).filter(field => {
    const baseType = getNonNullType(field.type);

    return isListType(baseType) && baseType.ofType instanceof GraphQLScalarType;
  });
}

function formatItem(
  item: Record<string, any>,
  jsonFields: GraphQLField<any, any>[]
) {
  const formatted = { ...item };

  jsonFields.forEach(field => {
    if (typeof formatted[field.name] === 'string') {
      formatted[field.name] = JSON.parse(formatted[field.name]);
    }
  });

  return formatted;
}
