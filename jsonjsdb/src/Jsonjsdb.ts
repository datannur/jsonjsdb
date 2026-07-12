import DBrowser from './DBrowser'
import Loader from './Loader'
import IntegrityChecker from './IntegrityChecker'
import type {
  JsonjsdbConfig,
  PartialJsonjsdbConfig,
  IntegrityResult,
  Schema,
  DatabaseMetadata,
  DatabaseRow,
  TableCollection,
  FilterBuilder,
} from './types'
import {
  relationFieldToKey,
  relationFieldToReverseKey,
  relationKeyToField,
  resolveRelationField,
} from './relationResolver'

type InitOption = {
  filterBuilder?: FilterBuilder
  useCache?: boolean
  version?: number | string
  limit?: number
  validIdChars?: string
}

type LoadOption = {
  useCache?: boolean
  version?: number | string
  shouldStandardizeIds?: boolean
  shouldTransformKeys?: boolean
}

type ForeignTableObj = {
  [tableName: string]: string | number | { id: string | number } | undefined
}

export type AddRelationOptions = {
  ifExists?: 'throw' | 'ignore'
}

export type AddRelationsResult = {
  added: Array<string | number>
  ignored: Array<string | number>
}

type IndexBucket = number | number[]
type IndexRecord = Record<string | number, IndexBucket>

export default class Jsonjsdb<
  TEntityTypeMap extends Record<string, DatabaseRow> = Record<
    string,
    DatabaseRow
  >,
> {
  defaultConfig: JsonjsdbConfig
  config!: JsonjsdbConfig
  browser: DBrowser
  loader: Loader
  integrityChecker: IntegrityChecker
  tables!: TableCollection<TEntityTypeMap>
  metadata!: DatabaseMetadata
  private idSuffix = 'Id'
  private usageCache: Partial<Record<keyof TEntityTypeMap, boolean>> = {}
  private recursiveUsageCache: Partial<Record<keyof TEntityTypeMap, boolean>> =
    {}
  private relationTableNames: string[] = []

  constructor(config?: string | PartialJsonjsdbConfig) {
    this.defaultConfig = {
      path: 'db',
      dbKey: false,
      appName: 'jsonjsdb',
      useCache: false,
      validIdChars: 'a-zA-Z0-9_, -',
    }

    let processedConfig: PartialJsonjsdbConfig = {}
    if (typeof config === 'string') {
      const htmlConfig = this.getHtmlConfig(config)
      processedConfig = htmlConfig || {}
    } else if (config) {
      processedConfig = config
    }

    this.setConfig(processedConfig)
    this.browser = new DBrowser(this.config.appName)
    this.loader = new Loader(this.browser, this.config)
    this.integrityChecker = new IntegrityChecker()
  }
  private getHtmlConfig(
    id = '#jsonjsdb-config',
  ): PartialJsonjsdbConfig | false {
    const configElement = document.querySelector(id) as HTMLElement
    if (!configElement) return false

    const { dataset } = configElement
    const config: PartialJsonjsdbConfig = {}

    if (dataset.path) config.path = dataset.path
    if (dataset.dbKey)
      config.dbKey = dataset.dbKey === 'false' ? false : dataset.dbKey
    if (dataset.appName) config.appName = dataset.appName
    if (dataset.useCache) config.useCache = dataset.useCache === 'true'
    if (dataset.validIdChars) config.validIdChars = dataset.validIdChars

    return config
  }
  private setConfig(config: PartialJsonjsdbConfig): void {
    this.config = { ...this.defaultConfig, ...config }

    if (window?.location.protocol.startsWith('http')) {
      this.config.useCache = false
    }
    if (this.config.dbKey) {
      this.config.path += '/' + this.config.dbKey
    }
  }

  async init(option: InitOption = {}): Promise<Jsonjsdb<TEntityTypeMap>> {
    this.tables = (await this.loader.load(
      this.config.path,
      this.config.useCache,
      option,
    )) as TableCollection<TEntityTypeMap>
    this.metadata = this.loader.metadata
    this.relationTableNames = this.getRelationTableNames()

    this.computeUsage()
    return this
  }
  async load(
    filePath: string,
    name: string,
    option: boolean | LoadOption = true,
  ): Promise<unknown[]> {
    const loadOption =
      typeof option === 'boolean' ? { shouldStandardizeIds: option } : option

    filePath = this.config.path + '/' + filePath
    const data = await this.loader.loadJsonjs(filePath, name, {
      useCache: loadOption.useCache ?? false,
      version: loadOption.version ?? Date.now(),
      shouldStandardizeIds: loadOption.shouldStandardizeIds,
      shouldTransformKeys: loadOption.shouldTransformKeys,
    })
    return data
  }

  get<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
  ): TEntityTypeMap[K] | undefined {
    try {
      const tableData = this.tables[table]
      if (!Array.isArray(tableData)) return undefined

      const indexValue = this.metadata.index[table].id[id]
      if (typeof indexValue !== 'number') return undefined

      const result = tableData[indexValue]
      if (!result) {
        console.error(`table ${table}, id not found: ${id}`)
      }
      return result
    } catch {
      if (!this.tables[table]) {
        console.error(`table ${table} not found`)
      } else if (!this.metadata.index[table]) {
        console.error(`table ${table} not found in index`)
      } else if (!('id' in this.metadata.index[table])) {
        console.error(`table ${table}, props "id" not found in index`)
      } else {
        console.error(`error not handled`)
      }
      return undefined
    }
  }
  getAll<K extends keyof TEntityTypeMap>(
    table: K & string,
    foreignTableObj?: ForeignTableObj,
    option: InitOption = {},
  ): TEntityTypeMap[K][] {
    const tableData = this.tables[table]
    if (!Array.isArray(tableData)) return []

    if (!foreignTableObj) {
      if (option.limit) {
        return tableData.slice(0, option.limit)
      }
      return tableData
    }

    const foreignTableObjStart = Object.entries(foreignTableObj)[0]
    const foreignTable = foreignTableObjStart[0]
    let foreignValue: string | number | { id: string | number } | undefined =
      foreignTableObjStart[1]

    if (foreignValue === undefined) return []

    if (
      typeof foreignValue === 'object' &&
      foreignValue !== null &&
      'id' in foreignValue
    ) {
      foreignValue = foreignValue.id
    } else {
      foreignValue = foreignValue as string | number
    }

    const foreignKey = this.resolveRelationIndexField(table, foreignTable)
    if (!foreignKey) return []

    const indexAll = this.metadata.index[table][foreignKey]
    if (!indexAll || !(foreignValue in indexAll)) return []
    const indexes = indexAll[foreignValue]

    if (!Array.isArray(indexes)) {
      if (typeof indexes !== 'number' || !tableData[indexes]) {
        console.error('getAll() table', table, 'has an index undefined')
        return []
      }
      return [tableData[indexes]]
    }

    const variables = []
    for (const index of indexes) {
      if (option.limit && variables.length >= option.limit) break
      if (!tableData[index]) {
        console.error('getAll() table', table, 'has an index undefined')
        continue
      }
      variables.push(tableData[index])
    }
    return variables
  }

  private resolveRelationIndexField(
    table: string,
    relationKey: string,
  ): string | null {
    if (relationKey === table) return 'parent' + this.idSuffix

    const tableIndex = this.metadata.index[table]
    if (!tableIndex) return null

    if (relationKey in tableIndex) return relationKey

    const relationField = relationKeyToField(relationKey)
    if (relationField in tableIndex) return relationField

    return null
  }
  getAllChilds<K extends keyof TEntityTypeMap>(
    table: K & string,
    itemId: string | number,
  ): TEntityTypeMap[K][] {
    const tableData = this.tables[table]
    if (!Array.isArray(tableData)) return []

    let all: TEntityTypeMap[K][] = []
    if (!itemId) {
      console.error('getAllChilds()', table, 'id', itemId)
      return all
    }
    const childs = this.getAll(table, { [table]: itemId })
    all = all.concat(childs)
    for (const child of childs) {
      const childRow = child
      if (itemId === childRow.id) {
        const msg = 'infinite loop for id'
        console.error('getAllChilds()', table, msg, itemId)
        return all
      }
      const newChilds = this.getAllChilds(table, childRow.id as string | number)
      all = all.concat(newChilds)
    }
    return all
  }
  foreach<K extends keyof TEntityTypeMap>(
    table: K & string,
    callback: (row: TEntityTypeMap[K]) => void,
  ): void {
    const rows = this.getAll(table)
    for (const row of rows) callback(row)
  }
  exists<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
  ): boolean {
    if (!this.tables[table]) return false
    if (!this.metadata.index[table]) return false
    if (!this.metadata.index[table].id) return false
    return id in this.metadata.index[table].id
  }
  getConfig(id: string | number): string | number | undefined {
    const configTable = this.tables['config']
    if (!Array.isArray(configTable)) return undefined

    const index = this.metadata.index['config'].id
    if (!index) return undefined
    if (!(id in index)) return undefined

    const indexValue = index[id]
    if (typeof indexValue !== 'number') return undefined

    const row = configTable[indexValue]
    return row['value'] as string | number
  }
  countRelated<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    relatedTable: string,
    relationKey?: string,
  ): number {
    if (!(relatedTable in this.metadata.index)) return 0
    const indexField = relationKey
      ? this.resolveRelationIndexField(relatedTable, relationKey)
      : table + this.idSuffix
    if (!indexField) return 0
    const index = this.metadata.index[relatedTable][indexField]
    if (!index) return 0
    if (!(id in index)) return 0
    const indexValue = index[id]
    if (!Array.isArray(indexValue)) return 1
    return indexValue.length
  }
  update<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    patch: Partial<TEntityTypeMap[K]>,
  ): TEntityTypeMap[K] | undefined {
    this.assertPatchAllowed(patch)

    const row = this.get(table, id)
    if (!row) return undefined

    Object.assign(row, patch)
    return row
  }
  insert<K extends keyof TEntityTypeMap>(
    table: K & string,
    row: TEntityTypeMap[K],
  ): TEntityTypeMap[K] {
    const tableData = this.getMutableTable(table)
    if (row.id == null || row.id === '') {
      throw new Error(`insert() table ${table} row id is required`)
    }
    if (this.exists(table, row.id)) {
      throw new Error(`insert() table ${table} duplicate id: ${String(row.id)}`)
    }

    const position = tableData.length
    tableData.push(row)
    this.ensureTableIndex(table).id ??= {}
    this.ensureTableIndex(table).id[row.id] = position

    this.addInsertedRowIndexes(table, row, position)
    this.computeUsage()
    return row
  }
  addRelation<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    relationField: string,
    relatedId: string | number,
    options: AddRelationOptions = {},
  ): boolean {
    const ifExists = options.ifExists ?? 'throw'
    if (!relationField.endsWith(this.idSuffix + 's')) {
      throw new Error(`addRelation() field must end with ${this.idSuffix}s`)
    }

    const sourceRow = this.get(table, id)
    if (!sourceRow) {
      throw new Error(
        `addRelation() table ${table} id not found: ${String(id)}`,
      )
    }

    const relatedTable = this.relationFieldToTable(relationField)
    if (!this.get(relatedTable, relatedId)) {
      throw new Error(
        `addRelation() table ${relatedTable} id not found: ${String(relatedId)}`,
      )
    }
    if (this.hasRelation(table, id, relatedTable, relatedId, relationField)) {
      if (ifExists === 'ignore') return false
      throw new Error(
        `addRelation() relation already exists: ${table}.${relationField}`,
      )
    }

    this.appendRelation(table, id, relatedTable, relatedId, relationField)
    this.appendRelationFieldValue(sourceRow, relationField, relatedId)
    return true
  }
  addRelations<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    relationField: string,
    relatedIds: Array<string | number>,
    options: AddRelationOptions = {},
  ): AddRelationsResult {
    const ifExists = options.ifExists ?? 'throw'
    if (!relationField.endsWith(this.idSuffix + 's')) {
      throw new Error(`addRelations() field must end with ${this.idSuffix}s`)
    }

    const sourceRow = this.get(table, id)
    if (!sourceRow) {
      throw new Error(
        `addRelations() table ${table} id not found: ${String(id)}`,
      )
    }

    const relatedTable = this.relationFieldToTable(relationField)
    const planned = this.planRelations(
      table,
      id,
      relatedTable,
      relatedIds,
      ifExists,
      relationField,
    )

    for (const relatedId of planned.added) {
      this.appendRelation(table, id, relatedTable, relatedId, relationField)
    }
    this.appendRelationFieldValues(sourceRow, relationField, planned.added)

    return planned
  }
  addForeignKey<K extends keyof TEntityTypeMap>(
    table: K & string,
    id: string | number,
    relationField: string,
    relatedId: string | number,
  ): boolean {
    if (
      !relationField.endsWith(this.idSuffix) ||
      relationField.endsWith(this.idSuffix + 's')
    ) {
      throw new Error(`addForeignKey() field must end with ${this.idSuffix}`)
    }

    const sourceRow = this.get(table, id)
    if (!sourceRow) {
      throw new Error(
        `addForeignKey() table ${table} id not found: ${String(id)}`,
      )
    }

    const relation = this.resolveRelationField(relationField)
    if (relation.many) {
      throw new Error(`addForeignKey() field must end with ${this.idSuffix}`)
    }

    const mutableSourceRow = sourceRow as DatabaseRow
    const currentValue = mutableSourceRow[relationField]
    if (currentValue != null && currentValue !== '') {
      throw new Error(
        `addForeignKey() foreign key already exists: ${table}.${relationField}`,
      )
    }

    if (!this.get(relation.toTable, relatedId)) {
      throw new Error(
        `addForeignKey() table ${relation.toTable} id not found: ${String(relatedId)}`,
      )
    }

    mutableSourceRow[relationField] = relatedId
    this.addPositionToIndex(
      this.ensureIndex(table, relationField),
      relatedId,
      this.getRowPosition(table, id),
    )
    return true
  }
  getParents<K extends keyof TEntityTypeMap>(
    from: K & string,
    id: string | number,
  ): TEntityTypeMap[K][] {
    if (!id || id === null) return []
    let parent = this.get(from, id)
    if (!parent) return []

    const parents: TEntityTypeMap[K][] = []
    const iterationMax = 30
    let iterationNum = 0

    while (iterationNum < iterationMax) {
      iterationNum += 1
      const parentRow = parent as TEntityTypeMap[K]
      const parentId = parentRow['parent' + this.idSuffix]

      if (!parentId && parentId !== 0) return parents.reverse()

      const parentBefore = parent
      parent = this.get(from, parentId as string | number)
      if (!parent) {
        const parentBeforeRow = parentBefore
        console.error(
          'getParents() type',
          from,
          'cannot find id',
          parentBeforeRow['parent' + this.idSuffix],
        )
        return []
      }
      parents.push(parent)
    }
    console.error('getParents()', from, id, 'iterationMax reached')
    return []
  }
  addMeta(
    userData?: Record<string, unknown>,
    dbSchema?: Record<string, unknown>[],
  ): void {
    this.loader.addMeta(userData, dbSchema)
  }
  getLastModifTimestamp(): number {
    return this.loader.getLastModifTimestamp()
  }

  get use(): Partial<Record<keyof TEntityTypeMap, boolean>> {
    return this.usageCache
  }

  get useRecursive(): Partial<Record<keyof TEntityTypeMap, boolean>> {
    return this.recursiveUsageCache
  }

  private computeUsage(): void {
    this.usageCache = {}
    this.recursiveUsageCache = {}

    for (const entity in this.tables) {
      const table = this.tables[entity]
      if (!Array.isArray(table)) continue
      if (table.length === 0) continue
      if (entity.includes('_')) continue
      ;(this.usageCache as Record<string, boolean>)[entity] = true

      const firstItem = table[0]
      if (
        firstItem &&
        typeof firstItem === 'object' &&
        'parent' + this.idSuffix in firstItem
      ) {
        ;(this.recursiveUsageCache as Record<string, boolean>)[entity] = true
      }
    }
  }

  getSchema(): Schema {
    return structuredClone(this.metadata.schema)
  }

  async checkIntegrity(): Promise<IntegrityResult> {
    await this.loader.loadTables(this.config.path, false)
    return this.integrityChecker.check(
      this.loader.db,
      this.loader.metadata.tables,
    )
  }

  private getMutableTable(table: string): DatabaseRow[] {
    const tableData = (this.tables as Record<string, DatabaseRow[]>)[table]
    if (!Array.isArray(tableData)) {
      throw new Error(`table ${table} not found`)
    }
    return tableData
  }

  private ensureTableIndex(table: string) {
    this.metadata.index[table] ??= {}
    return this.metadata.index[table]
  }

  private assertPatchAllowed(patch: Record<string, unknown>): void {
    for (const field of Object.keys(patch)) {
      if (
        field === 'id' ||
        field === 'parent' + this.idSuffix ||
        field.endsWith(this.idSuffix) ||
        field.endsWith(this.idSuffix + 's')
      ) {
        throw new Error(
          `update() cannot update indexed or relational field: ${field}`,
        )
      }
    }
  }

  private addInsertedRowIndexes(
    table: string,
    row: DatabaseRow,
    position: number,
  ): void {
    for (const [field, value] of Object.entries(row)) {
      if (field === 'id') continue

      if (field === 'parent' + this.idSuffix) {
        if (value != null && value !== '') {
          this.addPositionToIndex(
            this.ensureIndex(table, field),
            value as string | number,
            position,
          )
        }
        continue
      }

      if (field.endsWith(this.idSuffix + 's')) {
        const relation = this.resolveRelationField(field)
        for (const relatedId of this.parseIds(value)) {
          this.appendRelation(
            table,
            row.id as string | number,
            relation.toTable,
            relatedId,
            field,
          )
        }
        continue
      }

      if (
        field.endsWith(this.idSuffix) &&
        this.metadata.index[table]?.[field]
      ) {
        if (value != null && value !== '') {
          this.addPositionToIndex(
            this.ensureIndex(table, field),
            value as string | number,
            position,
          )
        }
      }
    }
  }

  private ensureIndex(table: string, field: string): IndexRecord {
    const tableIndex = this.ensureTableIndex(table)
    tableIndex[field] ??= {}
    return tableIndex[field] as IndexRecord
  }

  private addPositionToIndex(
    index: IndexRecord,
    key: string | number,
    position: number,
  ): void {
    if (!(key in index)) {
      index[key] = position
      return
    }

    const bucket = index[key]
    if (Array.isArray(bucket)) {
      if (!bucket.includes(position)) bucket.push(position)
      return
    }

    if (bucket !== position) index[key] = [bucket, position]
  }

  private relationFieldToTable(relationField: string): string {
    return this.resolveRelationField(relationField).toTable
  }

  private resolveRelationField(relationField: string) {
    const relation = resolveRelationField(
      relationField,
      this.relationTableNames,
    )
    if (!relation) {
      throw new Error(`relation table not found for field: ${relationField}`)
    }
    return relation
  }

  private getRelationTableNames(): string[] {
    return Object.entries(this.tables)
      .filter(([, tableData]) => Array.isArray(tableData))
      .map(([tableName]) => tableName)
  }

  private relationTableName(table: string, relatedTable: string): string {
    return `${table}_${relatedTable}`
  }

  private appendRelation(
    table: string,
    id: string | number,
    relatedTable: string,
    relatedId: string | number,
    relationField?: string,
  ): void {
    if (this.hasRelation(table, id, relatedTable, relatedId, relationField))
      return

    const relation = relationField
      ? this.resolveRelationField(relationField)
      : null

    if (relation?.role) {
      const sourcePosition = this.getRowPosition(table, id)
      const relatedPosition = this.getRowPosition(relatedTable, relatedId)
      this.addPositionToIndex(
        this.ensureIndex(table, relationFieldToKey(relationField!)),
        relatedId,
        sourcePosition,
      )
      this.addPositionToIndex(
        this.ensureIndex(relatedTable, relationFieldToReverseKey(relation)),
        id,
        relatedPosition,
      )
      return
    }

    const relationTable = this.ensureRelationTable(table, relatedTable)
    const sourcePosition = this.getRowPosition(table, id)
    const relatedPosition = this.getRowPosition(relatedTable, relatedId)

    relationTable.push({
      [table + this.idSuffix]: id,
      [relatedTable + this.idSuffix]: relatedId,
    })
    this.addPositionToIndex(
      this.ensureIndex(table, relatedTable + this.idSuffix),
      relatedId,
      sourcePosition,
    )
    this.addPositionToIndex(
      this.ensureIndex(relatedTable, table + this.idSuffix),
      id,
      relatedPosition,
    )
  }

  private ensureRelationTable(
    table: string,
    relatedTable: string,
  ): DatabaseRow[] {
    const relationTableName = this.relationTableName(table, relatedTable)
    const dbTables = this.tables as Record<string, DatabaseRow[]>
    if (!Array.isArray(dbTables[relationTableName])) {
      dbTables[relationTableName] = []
      this.metadata.tables.push({ name: relationTableName })
    }
    return dbTables[relationTableName]
  }

  private getRowPosition(table: string, id: string | number): number {
    const tableIndex = this.metadata.index[table]
    const indexValue = tableIndex?.id?.[id]
    if (typeof indexValue !== 'number') {
      throw new Error(`table ${table}, id not found: ${String(id)}`)
    }
    return indexValue
  }

  private hasRelation(
    table: string,
    id: string | number,
    relatedTable: string,
    relatedId: string | number,
    relationField?: string,
  ): boolean {
    if (relationField) {
      const relation = this.resolveRelationField(relationField)
      if (relation.role) {
        const sourceRow = this.get(table, id)
        if (!sourceRow) return false
        return this.parseIds(sourceRow[relationField]).some(existingId =>
          this.sameId(existingId, relatedId),
        )
      }
    }

    const positions =
      this.metadata.index[table]?.[relatedTable + this.idSuffix]?.[relatedId]
    if (positions === undefined) return false
    const sourcePosition = this.metadata.index[table]?.id?.[id]
    if (typeof sourcePosition !== 'number') return false
    return Array.isArray(positions)
      ? positions.includes(sourcePosition)
      : positions === sourcePosition
  }

  private planRelations(
    table: string,
    id: string | number,
    relatedTable: string,
    relatedIds: Array<string | number>,
    ifExists: 'throw' | 'ignore',
    relationField?: string,
  ): AddRelationsResult {
    const added: Array<string | number> = []
    const ignored: Array<string | number> = []

    for (const relatedId of relatedIds) {
      const relatedRow = this.get(relatedTable, relatedId)
      if (!relatedRow) {
        throw new Error(
          `addRelations() related table ${relatedTable} id not found: ${String(relatedId)}`,
        )
      }

      const alreadyExists =
        this.hasRelation(table, id, relatedTable, relatedId, relationField) ||
        added.some(addedId => this.sameId(addedId, relatedId))

      if (alreadyExists) {
        if (ifExists === 'ignore') {
          ignored.push(relatedId)
          continue
        }

        throw new Error(`addRelations() relation already exists`)
      }

      added.push(relatedId)
    }

    return { added, ignored }
  }

  private appendRelationFieldValue(
    row: DatabaseRow,
    relationField: string,
    relatedId: string | number,
  ): void {
    this.appendRelationFieldValues(row, relationField, [relatedId])
  }

  private appendRelationFieldValues(
    row: DatabaseRow,
    relationField: string,
    relatedIds: Array<string | number>,
  ): void {
    const ids = this.parseIds(row[relationField])
    for (const relatedId of relatedIds) {
      if (ids.some(id => this.sameId(id, relatedId))) continue
      ids.push(relatedId)
    }
    row[relationField] = this.serializeIds(ids)
  }

  private sameId(left: unknown, right: unknown): boolean {
    return String(left) === String(right)
  }

  private parseIds(value: unknown): Array<string | number> {
    if (Array.isArray(value)) {
      return value.filter(
        (item): item is string | number =>
          typeof item === 'string' || typeof item === 'number',
      )
    }

    if (typeof value === 'number') return [value]
    if (typeof value !== 'string') return []

    return value
      .split(',')
      .map(id => id.trim())
      .filter(id => id !== '')
      .map(id => {
        const numericId = Number(id)
        return Number.isNaN(numericId) ? id : numericId
      })
  }

  private serializeIds(ids: Array<string | number>): string {
    return ids.map(id => String(id)).join(', ')
  }
}
