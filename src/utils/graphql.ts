import {
  GraphQLField,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  isLeafType,
  isListType,
  parse,
  print,
  visit
} from 'graphql';
import { jsonToGraphQLQuery } from 'json-to-graphql-query';
import pluralize from 'pluralize';
import { ComputedResolvers } from '../types';

const directiveDef = (name: string, args: any[] = []) => ({
  kind: 'DirectiveDefinition',
  name: { kind: 'Name', value: name },
  arguments: args,
  locations: [{ kind: 'Name', value: 'FIELD_DEFINITION' }]
});

const nonNullStringArg = (name: string) => ({
  kind: 'InputValueDefinition',
  name: { kind: 'Name', value: name },
  type: {
    kind: 'NonNullType',
    type: { kind: 'NamedType', name: { kind: 'Name', value: 'String' } }
  }
});

export const extendSchema = (schema: string): string => {
  const ast = parse(schema);

  const updatedAst = visit(ast, {
    Document(node) {
      return {
        ...node,
        definitions: [
          directiveDef('derivedFrom', [nonNullStringArg('field')]),
          directiveDef('computed'),
          ...node.definitions
        ]
      };
    },
    ObjectTypeDefinition(node) {
      const indexerField = {
        kind: 'FieldDefinition',
        name: { kind: 'Name', value: '_indexer' },
        type: {
          kind: 'NonNullType',
          type: { kind: 'NamedType', name: { kind: 'Name', value: 'String' } }
        }
      };

      return {
        ...node,
        fields: node.fields ? [...node.fields, indexerField] : [indexerField]
      };
    }
  });

  return print(updatedAst);
};

/**
 * Returns name of query for fetching single entity record
 *
 */
export const singleEntityQueryName = (entity: GraphQLObjectType) =>
  entity.name.toLowerCase();

/**
 * Returns name of query for fetching multiple entity records
 *
 */
export const multiEntityQueryName = (entity: GraphQLObjectType) => {
  if (entity.name === '_Metadata') return '_metadatas';

  return pluralize(entity.name.toLowerCase());
};

/**
 * Generate sample query string based on entity object fields.
 *
 */
export const generateQueryForEntity = (entity: GraphQLObjectType): string => {
  // function to recursively build fields map
  const getObjectFields = (
    object: GraphQLObjectType,
    queryFields = {}
  ): Record<string, any> => {
    const objectFields = object.getFields();

    Object.keys(objectFields).forEach(fieldName => {
      const rawFieldType = objectFields[fieldName].type;
      const fieldType =
        rawFieldType instanceof GraphQLNonNull
          ? rawFieldType.ofType
          : rawFieldType;

      if (isLeafType(fieldType)) {
        queryFields[fieldName] = true;
      } else if (isListType(fieldType)) {
        if (fieldType.ofType instanceof GraphQLScalarType) {
          queryFields[fieldName] = true;
        }
      } else {
        const childObjectFields = {};
        getObjectFields(fieldType as GraphQLObjectType, childObjectFields);
        queryFields[fieldName] = childObjectFields;
      }
    });

    return queryFields;
  };

  return jsonToGraphQLQuery(
    {
      query: {
        [multiEntityQueryName(entity)]: {
          __args: { first: 10 },
          ...getObjectFields(entity)
        }
      }
    },
    { pretty: true }
  );
};

export const getNonNullType = <T>(type: T): T => {
  if (type instanceof GraphQLNonNull) {
    return type.ofType;
  }

  return type;
};

const getDirective = (field: GraphQLField<any, any>, name: string) =>
  (field.astNode?.directives ?? []).find(d => d.name.value === name);

export const getDerivedFromDirective = (f: GraphQLField<any, any>) =>
  getDirective(f, 'derivedFrom');

export const getComputedDirective = (f: GraphQLField<any, any>) =>
  getDirective(f, 'computed');

export const getComputedConfigs = (
  resolvers: ComputedResolvers | undefined,
  typeName: string
) =>
  resolvers
    ? Object.entries(resolvers).find(
        ([k]) => k.toLowerCase() === typeName.toLowerCase()
      )?.[1] || {}
    : {};
