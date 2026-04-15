import { register } from '../register';

export default class Model {
  private tableName: string;
  private indexerName: string;
  private values = new Map<string, any>();
  private exists = false;

  constructor(tableName: string, indexerName: string) {
    this.tableName = tableName;
    this.indexerName = indexerName;
  }

  setExists() {
    this.exists = true;
  }

  initialSet(key: string, value: any) {
    this.values.set(key, value);
  }

  get(key: string): any {
    return this.values.get(key) ?? null;
  }

  set(key: string, value: any) {
    this.values.set(key, value);
  }

  static async _loadEntity(
    tableName: string,
    id: string | number,
    indexerName: string
  ): Promise<Record<string, any> | null> {
    return register.getBuffer(indexerName).loadEntity(tableName, id);
  }

  async save() {
    const values = Object.fromEntries(this.values.entries());
    const id = this.get('id');
    const buffer = register.getBuffer(this.indexerName);

    if (this.exists) {
      buffer.stageUpdate(this.tableName, id, values);
    } else {
      buffer.stageInsert(this.tableName, id, values);
    }
  }

  async delete() {
    if (!this.exists) return;
    register
      .getBuffer(this.indexerName)
      .stageDelete(this.tableName, this.get('id'));
  }
}
