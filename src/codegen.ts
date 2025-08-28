import {
  GraphQLField,
  GraphQLFloat,
  GraphQLID,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLString,
  GraphQLType,
  isListType
} from 'graphql';
import pluralize from 'pluralize';
import { GqlEntityController } from './graphql/controller';
import { OverridesConfig } from './types';
import { getDerivedFromDirective } from './utils/graphql';

type TypeInfo = {
  type: string;
  initialValue: any;
};

type DecimalTypes = NonNullable<OverridesConfig['decimal_types']>;

const DEFAULT_DECIMAL_TYPES = {
  Decimal: {
    p: 10,
    d: 2
  },
  BigDecimal: {
    p: 20,
    d: 8
  }
};

export const getTypeInfo = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
): TypeInfo => {
  if (type instanceof GraphQLNonNull) {
    throw new Error('Type must raw type');
  }

  switch (type) {
    case GraphQLInt:
    case GraphQLFloat:
      return { type: 'number', initialValue: 0 };
    case GraphQLString:
    case GraphQLID:
      return { type: 'string', initialValue: '' };
  }

  if (type instanceof GraphQLScalarType) {
    switch (type.name) {
      case 'BigInt':
        return { type: 'bigint', initialValue: 0 };
      case 'Boolean':
        return { type: 'boolean', initialValue: false };
      case 'Text':
        return { type: 'string', initialValue: '' };
    }

    if (type.name in decimalTypes) {
      return { type: 'string', initialValue: '0' };
    }
  }

  if (type instanceof GraphQLObjectType) {
    return { type: 'string', initialValue: '' };
  }

  if (type instanceof GraphQLList) {
    const nonNullNestedType =
      type.ofType instanceof GraphQLNonNull ? type.ofType.ofType : type.ofType;

    return {
      type: `${getTypeInfo(nonNullNestedType, decimalTypes).type}[]`,
      initialValue: '[]'
    };
  }

  throw new Error('Unknown type');
};

export const getInitialValue = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  if (!(type instanceof GraphQLNonNull)) {
    return null;
  }

  return getTypeInfo(type.ofType, decimalTypes).initialValue;
};

export const getBaseType = (
  type: GraphQLType,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  const nonNullType = type instanceof GraphQLNonNull ? type.ofType : type;

  return getTypeInfo(nonNullType, decimalTypes).type;
};

export const getJSType = (
  field: GraphQLField<any, any>,
  decimalTypes: DecimalTypes = DEFAULT_DECIMAL_TYPES
) => {
  const nonNullType =
    field.type instanceof GraphQLNonNull ? field.type.ofType : field.type;
  const isNullable = !(field.type instanceof GraphQLNonNull);
  const isList = nonNullType instanceof GraphQLList;
  const baseType = getBaseType(nonNullType, decimalTypes);

  return { isNullable, isList, baseType };
};

export const codegen = (
  controller: GqlEntityController,
  config: OverridesConfig,
  format: 'typescript' | 'javascript'
) => {
  const decimalTypes = config.decimal_types || DEFAULT_DECIMAL_TYPES;

  const preamble = `import { Model } from '@snapshot-labs/checkpoint';\n\n`;

  let contents = `${preamble}`;

  controller.schemaObjects.forEach((type, i, arr) => {
    const modelName = type.name;

    contents += `export class ${modelName} extends Model {\n`;
    contents += `  static tableName = '${pluralize(modelName.toLowerCase())}';\n\n`;

    const typeFields = controller.getTypeFields(type);
    const idField = typeFields.find(field => field.name === 'id');
    const idType = idField ? getJSType(idField, decimalTypes) : null;

    if (
      !idType ||
      !['string', 'number'].includes(idType.baseType) ||
      idType.isNullable ||
      idType.isList
    ) {
      throw new Error(
        `Model ${modelName} must have an id field of type string or number`
      );
    }

    contents +=
      format === 'javascript'
        ? `  constructor(id, indexerName) {\n`
        : `  constructor(id: ${idType.baseType}, indexerName: string) {\n`;
    contents += `    super(${modelName}.tableName, indexerName);\n\n`;
    typeFields.forEach(field => {
      const fieldType =
        field.type instanceof GraphQLNonNull ? field.type.ofType : field.type;
      if (
        isListType(fieldType) &&
        fieldType.ofType instanceof GraphQLObjectType &&
        getDerivedFromDirective(field)
      ) {
        return;
      }

      const rawInitialValue = getInitialValue(field.type, decimalTypes);
      const initialValue =
        field.name === 'id' ? 'id' : JSON.stringify(rawInitialValue);
      contents += `    this.initialSet('${field.name}', ${initialValue});\n`;
    });
    contents += `  }\n\n`;

    contents +=
      format === 'javascript'
        ? `  static async loadEntity(id, indexerName) {\n`
        : `  static async loadEntity(id: ${idType.baseType}, indexerName: string): Promise<${modelName} | null> {\n`;
    contents += `    const entity = await super._loadEntity(${modelName}.tableName, id, indexerName);\n`;
    contents += `    if (!entity) return null;\n\n`;
    contents += `    const model = new ${modelName}(id, indexerName);\n`;
    contents += `    model.setExists();\n\n`;
    contents += `    for (const key in entity) {\n`;
    contents += `      const value = entity[key] !== null && typeof entity[key] === 'object'\n`;
    contents += `        ? JSON.stringify(entity[key])\n`;
    contents += `        : entity[key];\n`;
    contents += `      model.set(key, value);\n`;
    contents += `    }\n\n`;
    contents += `    return model;\n`;
    contents += `  }\n\n`;

    typeFields.forEach(field => {
      const fieldType =
        field.type instanceof GraphQLNonNull ? field.type.ofType : field.type;
      if (
        isListType(fieldType) &&
        fieldType.ofType instanceof GraphQLObjectType &&
        getDerivedFromDirective(field)
      ) {
        return;
      }

      const { isNullable, isList, baseType } = getJSType(field, decimalTypes);
      const typeAnnotation = isNullable ? `${baseType} | null` : baseType;

      contents +=
        format === 'javascript'
          ? `  get ${field.name}() {\n`
          : `  get ${field.name}(): ${typeAnnotation} {\n`;
      contents += `    return ${
        isList
          ? `JSON.parse(this.get('${field.name}'))`
          : `this.get('${field.name}')`
      };\n`;
      contents += `  }\n\n`;

      contents +=
        format === 'javascript'
          ? `  set ${field.name}(value) {\n`
          : `  set ${field.name}(value: ${typeAnnotation}) {\n`;
      contents += `    this.set('${field.name}', ${isList ? `JSON.stringify(value)` : 'value'});\n`;
      contents += `  }\n\n`;
    });

    contents = contents.slice(0, -1);
    contents += i === arr.length - 1 ? '}\n' : '}\n\n';
  });

  return contents;
};
