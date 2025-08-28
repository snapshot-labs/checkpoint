import { addResolversToSchema } from '@graphql-tools/schema';
import {
  buildSchema,
  GraphQLEnumType,
  GraphQLField,
  GraphQLFieldConfig,
  GraphQLFieldConfigMap,
  GraphQLFieldResolver,
  GraphQLFloat,
  GraphQLID,
  GraphQLInputObjectType,
  GraphQLInputObjectTypeConfig,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLOutputType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  isLeafType,
  isListType,
  Source
} from 'graphql';
import { Knex } from 'knex';
import pluralize from 'pluralize';
import { CheckpointsGraphQLObject, MetadataGraphQLObject } from '.';
import { KnexType } from '../knex';
import { OverridesConfig } from '../types';
import { getNestedResolver, queryMulti, querySingle } from './resolvers';
import {
  generateQueryForEntity,
  getDerivedFromDirective,
  getNonNullType,
  multiEntityQueryName,
  singleEntityQueryName
} from '../utils/graphql';

/**
 * Type for single and multiple query resolvers
 */
interface EntityQueryResolvers {
  singleEntityResolver: typeof querySingle;
  multipleEntityResolver: typeof queryMulti;
}

const GraphQLOrderDirection = new GraphQLEnumType({
  name: 'OrderDirection',
  values: {
    asc: { value: 'ASC' },
    desc: { value: 'DESC' }
  }
});

type WhereResult = {
  where: {
    type: GraphQLInputObjectType;
  };
  orderByValues: Record<string, { value: string }>;
};

/**
 * Controller for performing actions based on the graphql schema provided to its
 * constructor. It exposes public functions to generate graphql or database
 * items based on the entities identified in the schema.
 *
 * Note: Entities refer to Object types with an `id` field defined within the
 * graphql schema.
 */
export class GqlEntityController {
  private readonly schema: GraphQLSchema;
  private readonly decimalTypes: NonNullable<OverridesConfig['decimal_types']>;
  private _schemaObjects?: GraphQLObjectType[];

  constructor(typeDefs: string | Source, config?: OverridesConfig) {
    this.schema = buildSchema(typeDefs);
    this.decimalTypes = config?.decimal_types || {
      Decimal: {
        p: 10,
        d: 2
      },
      BigDecimal: {
        p: 20,
        d: 8
      }
    };
  }

  /**
   * Creates a grqphql Query object generated from the objects defined within
   * the schema.
   * For each of the objects, two queries are created, one for querying the object
   * by it's id and the second for querying multiple objects based on a couple
   * of parameters.
   *
   * For example, given the input schema:
   * ```
   * type Vote {
   *  id: Int!
   *  name: String
   * }
   * ```
   *
   * The generated queries will be like:
   * ```
   * type Query {
   *  votes(
   *     first: Int
   *     skip: Int
   *     orderBy: String
   *     orderDirection: String
   *     where: WhereVote
   *   ): [Vote]
   *   vote(id: Int!): Vote
   * }
   *
   *  input WhereVote {
   *    id: Int
   *    id_in: [Int]
   *    name: String
   *    name_in: [String]
   *  }
   *
   * ```
   *
   */
  public generateQueryFields(
    schemaObjects?: GraphQLObjectType[],
    resolvers: EntityQueryResolvers = {
      singleEntityResolver: querySingle,
      multipleEntityResolver: queryMulti
    }
  ): GraphQLFieldConfigMap<any, any> {
    schemaObjects = schemaObjects || this.schemaObjects;

    const queryFields: GraphQLFieldConfigMap<any, any> = {};

    schemaObjects.forEach(type => {
      queryFields[singleEntityQueryName(type)] =
        this.getSingleEntityQueryConfig(type, resolvers.singleEntityResolver);
      queryFields[multiEntityQueryName(type)] =
        this.getMultipleEntityQueryConfig(
          type,
          resolvers.multipleEntityResolver
        );
    });

    return queryFields;
  }

  /**
   * Generates entity resolvers for subqueries.
   * Returned resolvers use format compatible with addResolversToSchema.
   * {
   *   Proposal: {
   *     space: () => {}
   *   }
   * }
   */
  public generateEntityResolvers(fields: GraphQLFieldConfigMap<any, any>) {
    return this.schemaObjects.reduce((entities, obj) => {
      entities[obj.name] = this.getTypeFields(obj).reduce(
        (resolvers, field) => {
          const nonNullType = getNonNullType(field.type);

          if (isListType(nonNullType)) {
            const itemType = getNonNullType(nonNullType.ofType);

            if (itemType instanceof GraphQLObjectType) {
              resolvers[field.name] = getNestedResolver(
                multiEntityQueryName(itemType)
              );
            }
          }

          if (nonNullType instanceof GraphQLObjectType) {
            resolvers[field.name] =
              fields[singleEntityQueryName(nonNullType)].resolve;
          }

          return resolvers;
        },
        {}
      );

      return entities;
    }, {});
  }

  /**
   * Creates store for each of the objects in the schema.
   * For now, it only creates database tables for each of the objects.
   * It also creates a checkpoint table to track checkpoints visited.
   *
   * For example, given an schema like:
   * ```graphql
   * type Vote {
   *  id: Int!
   *  name: String
   * }
   * ```
   *
   * will execute the following SQL:
   * ```sql
   * DROP TABLE IF EXISTS votes;
   * CREATE TABLE votes (
   *   id VARCHAR(128) NOT NULL,
   *   name VARCHAR(128),
   *   PRIMARY KEY (id) ,
   *   INDEX id (id),
   *   INDEX name (name)
   * );
   * ```
   *
   */
  public async createEntityStores(
    knex: Knex
  ): Promise<{ builder: Knex.SchemaBuilder }> {
    let builder = knex.schema;

    if (knex.client.config.client === 'pg') {
      builder = builder.raw('CREATE EXTENSION IF NOT EXISTS btree_gist');
    }

    if (this.schemaObjects.length === 0) {
      return { builder };
    }

    this.schemaObjects.map(type => {
      const tableName = pluralize(type.name.toLowerCase());

      builder = builder
        .dropTableIfExists(tableName)
        .createTable(tableName, t => {
          t.uuid('uid').primary().defaultTo(knex.fn.uuid());
          t.specificType('block_range', 'int8range').notNullable();

          this.getTypeFields(type).forEach(field => {
            const fieldType =
              field.type instanceof GraphQLNonNull
                ? field.type.ofType
                : field.type;
            if (
              isListType(fieldType) &&
              fieldType.ofType instanceof GraphQLObjectType &&
              getDerivedFromDirective(field)
            ) {
              return;
            }
            const sqlType = this.getSqlType(field.type);

            let column =
              'options' in sqlType
                ? t[sqlType.name](field.name, ...sqlType.options)
                : t[sqlType.name](field.name);

            if (field.type instanceof GraphQLNonNull) {
              column = column.notNullable();
            }

            if (!['text', 'json'].includes(sqlType.name)) {
              column.index();
            }
          });
        });

      if (knex.client.config.client === 'pg') {
        builder = builder.raw(
          knex
            .raw(
              'ALTER TABLE ?? ADD EXCLUDE USING GIST (id WITH =, _indexer WITH =, block_range WITH &&)',
              [tableName]
            )
            .toQuery()
        );
      }
    });

    await builder;

    return { builder };
  }

  /**
   * Generates a query based on the first entity discovered
   * in a schema. If no entities are found in the schema
   * it returns undefined.
   *
   */
  public generateSampleQuery(): string | undefined {
    if (this.schemaObjects.length === 0) {
      return undefined;
    }

    const firstEntityQuery = generateQueryForEntity(this.schemaObjects[0]);
    const queryComment = `
# Welcome to Checkpoint. Try running the below example query from
# your defined entity.
    `;
    return `${queryComment}\n${firstEntityQuery}`;
  }

  /**
   * Returns a list of objects defined within the graphql typedefs.
   * The types returns are introspection objects, that can be used
   * for inspecting the fields and types.
   *
   * Note: that the returned objects does not include the Query object type if defined.
   *
   */
  public get schemaObjects(): GraphQLObjectType[] {
    if (this._schemaObjects) {
      return this._schemaObjects;
    }

    this._schemaObjects = Object.values(this.schema.getTypeMap()).filter(
      type => {
        return (
          type instanceof GraphQLObjectType &&
          type.name != 'Query' &&
          !type.name.startsWith('__')
        );
      }
    ) as GraphQLObjectType[];

    return this._schemaObjects;
  }

  public getTypeFields<Parent, Context>(
    type: GraphQLObjectType<Parent, Context>
  ): GraphQLField<Parent, Context>[] {
    return Object.values(type.getFields());
  }

  private getSingleEntityQueryConfig<Parent, Context>(
    type: GraphQLObjectType,
    resolver: GraphQLFieldResolver<Parent, Context>
  ): GraphQLFieldConfig<Parent, Context> {
    return {
      type,
      args: {
        id: {
          type: new GraphQLNonNull(this.getEntityIdType(type))
        },
        indexer: {
          type: GraphQLString
        },
        block: {
          type: GraphQLInt
        }
      },
      resolve: resolver
    };
  }

  private getMultipleEntityQueryConfig<Parent, Context>(
    type: GraphQLObjectType,
    resolver: GraphQLFieldResolver<Parent, Context>
  ): GraphQLFieldConfig<Parent, Context> {
    const getWhereType = (
      nestedType: GraphQLObjectType<any, any>,
      prefix?: string
    ): WhereResult => {
      const name = prefix
        ? `${prefix}_${nestedType.name}_filter`
        : `${nestedType.name}_filter`;

      const orderByValues = {};
      const whereInputConfig: GraphQLInputObjectTypeConfig = {
        name,
        fields: {}
      };

      this.getTypeFields(nestedType).forEach(field => {
        // all field types in a where input variable must be optional
        // so we try to extract the non null type here.
        let nonNullFieldType = getNonNullType(field.type);

        if (nonNullFieldType instanceof GraphQLObjectType) {
          const fields = nestedType.getFields();
          const idField = fields['id'];

          if (
            idField &&
            idField.type instanceof GraphQLNonNull &&
            idField.type.ofType instanceof GraphQLScalarType &&
            ['String', 'Int', 'ID'].includes(idField.type.ofType.name)
          ) {
            if (!prefix) {
              whereInputConfig.fields[`${field.name}_`] = getWhereType(
                nonNullFieldType,
                nestedType.name
              ).where;
            }

            nonNullFieldType = getNonNullType(idField.type);
          }
        }

        if (isListType(nonNullFieldType)) {
          const itemType = nonNullFieldType.ofType;

          if (!isLeafType(itemType)) {
            return;
          }

          whereInputConfig.fields[`${field.name}`] = { type: nonNullFieldType };
          whereInputConfig.fields[`${field.name}_not`] = {
            type: nonNullFieldType
          };

          if (itemType === GraphQLString) {
            // those are the only supported operators for string arrays because PostgreSQL intersection
            // for jsonb is only supported for string arrays
            whereInputConfig.fields[`${field.name}_contains`] = {
              type: nonNullFieldType
            };
            whereInputConfig.fields[`${field.name}_not_contains`] = {
              type: nonNullFieldType
            };
          }
        }

        // avoid setting up where filters for non scalar types
        if (!isLeafType(nonNullFieldType)) {
          return;
        }

        if (nonNullFieldType === GraphQLInt) {
          whereInputConfig.fields[`${field.name}_gt`] = { type: GraphQLInt };
          whereInputConfig.fields[`${field.name}_gte`] = { type: GraphQLInt };
          whereInputConfig.fields[`${field.name}_lt`] = { type: GraphQLInt };
          whereInputConfig.fields[`${field.name}_lte`] = { type: GraphQLInt };
        }

        if (
          (nonNullFieldType instanceof GraphQLScalarType &&
            nonNullFieldType.name === 'BigInt') ||
          this.decimalTypes[nonNullFieldType.name]
        ) {
          whereInputConfig.fields[`${field.name}_gt`] = {
            type: nonNullFieldType
          };
          whereInputConfig.fields[`${field.name}_gte`] = {
            type: nonNullFieldType
          };
          whereInputConfig.fields[`${field.name}_lt`] = {
            type: nonNullFieldType
          };
          whereInputConfig.fields[`${field.name}_lte`] = {
            type: nonNullFieldType
          };
        }

        if (
          nonNullFieldType === GraphQLString ||
          (nonNullFieldType as GraphQLScalarType).name === 'Text'
        ) {
          whereInputConfig.fields[`${field.name}_contains`] = {
            type: GraphQLString
          };
          whereInputConfig.fields[`${field.name}_not_contains`] = {
            type: GraphQLString
          };
          whereInputConfig.fields[`${field.name}_contains_nocase`] = {
            type: GraphQLString
          };
          whereInputConfig.fields[`${field.name}_not_contains_nocase`] = {
            type: GraphQLString
          };
        }

        if ((nonNullFieldType as GraphQLScalarType).name !== 'Text') {
          whereInputConfig.fields[`${field.name}`] = { type: nonNullFieldType };
          whereInputConfig.fields[`${field.name}_not`] = {
            type: nonNullFieldType
          };
          whereInputConfig.fields[`${field.name}_in`] = {
            type: new GraphQLList(nonNullFieldType)
          };
          whereInputConfig.fields[`${field.name}_not_in`] = {
            type: new GraphQLList(nonNullFieldType)
          };
        }

        orderByValues[field.name] = { value: field.name };
      });

      const result = {
        where: { type: new GraphQLInputObjectType(whereInputConfig) },
        orderByValues
      };

      return result;
    };

    const { where, orderByValues } = getWhereType(type);

    const OrderByEnum = new GraphQLEnumType({
      name: `${type.name}_orderBy`,
      values: orderByValues
    });

    return {
      type: new GraphQLNonNull(new GraphQLList(new GraphQLNonNull(type))),
      args: {
        first: {
          type: GraphQLInt
        },
        skip: {
          type: GraphQLInt
        },
        orderBy: {
          type: OrderByEnum
        },
        orderDirection: {
          type: GraphQLOrderDirection
        },
        indexer: {
          type: GraphQLString
        },
        block: {
          type: GraphQLInt
        },
        where
      },
      resolve: resolver
    };
  }

  private getEntityIdType(type: GraphQLObjectType): GraphQLScalarType {
    const idField = type.getFields().id;
    if (!idField) {
      throw new Error(
        `'id' field is missing in type '${type.name}'. All types are required to have an id field.`
      );
    }

    if (!(idField.type instanceof GraphQLNonNull)) {
      throw new Error(`'id' field for type ${type.name} must be non nullable.`);
    }

    const nonNullType = idField.type.ofType;

    // verify only scalar types are used
    if (!(nonNullType instanceof GraphQLScalarType)) {
      throw new Error(`'id' field for type ${type.name} is not a scalar type.`);
    }

    return nonNullType;
  }

  /**
   * Return a knex column type and options for the graphql type.
   *
   * It throws if the type is not a recognized type.
   */
  private getSqlType(type: GraphQLOutputType): KnexType {
    if (type instanceof GraphQLNonNull) {
      type = type.ofType;
    }

    switch (type) {
      case GraphQLInt:
        return { name: 'integer' };
      case GraphQLFloat:
        return { name: 'float', options: [23] };
      case GraphQLString:
      case GraphQLID:
        return { name: 'string', options: [256] };
    }

    if (type instanceof GraphQLObjectType) {
      const fields = type.getFields();
      const idField = fields['id'];

      if (
        idField &&
        idField.type instanceof GraphQLNonNull &&
        idField.type.ofType instanceof GraphQLScalarType
      ) {
        if (['String', 'ID'].includes(idField.type.ofType.name)) {
          return { name: 'string', options: [256] };
        } else if (idField.type.ofType.name === 'Int') {
          return { name: 'integer' };
        }
      }
    }

    // check for TEXT scalar type
    if (type instanceof GraphQLScalarType && type.name === 'Text') {
      return { name: 'text' };
    }

    if (type instanceof GraphQLScalarType && type.name === 'BigInt') {
      return { name: 'bigint' };
    }

    if (type instanceof GraphQLScalarType && type.name === 'Boolean') {
      return { name: 'boolean' };
    }

    if (type instanceof GraphQLScalarType && this.decimalTypes[type.name]) {
      const decimalType = this.decimalTypes[type.name];
      return { name: 'decimal', options: [decimalType.p, decimalType.d] };
    }

    if (type instanceof GraphQLList) {
      return { name: 'jsonb' };
    }

    throw new Error(`sql type for ${type} is not supported`);
  }

  generateSchema(opts?: { addResolvers?: boolean }) {
    const entityQueryFields = this.generateQueryFields();
    const coreQueryFields = this.generateQueryFields([
      MetadataGraphQLObject,
      CheckpointsGraphQLObject
    ]);

    const query = new GraphQLObjectType({
      name: 'Query',
      fields: {
        ...entityQueryFields,
        ...coreQueryFields
      }
    });

    const schema = new GraphQLSchema({ query });

    if (opts?.addResolvers) {
      return addResolversToSchema({
        schema,
        resolvers: this.generateEntityResolvers(entityQueryFields)
      });
    }

    return schema;
  }
}
