import type { IntegrityResult, TableRow } from './types'
import { resolveRelationField } from './relationResolver'

export default class IntegrityChecker {
  private idSuffix = 'Id'
  private tables: string[] = []
  private tablesIds: Record<string, (string | number | undefined)[]> = {}
  private result: IntegrityResult

  constructor() {
    this.tables = []
    this.tablesIds = {}
    this.result = {
      emptyId: [],
      duplicateId: {},
      parentIdNotFound: {},
      parentIdSame: {},
      foreignIdNotFound: {},
    }
  }

  check(
    db: Record<string, TableRow[]>,
    tables: { name: string }[],
  ): IntegrityResult {
    this.tables = []
    this.tablesIds = {}
    this.result = {
      emptyId: [],
      duplicateId: {},
      parentIdNotFound: {},
      parentIdSame: {},
      foreignIdNotFound: {},
    }

    for (const table of tables) {
      this.tables.push(table.name)
      const tableData = db[table.name] as TableRow[]
      this.tablesIds[table.name] = tableData.map(row => row.id)
    }

    for (const table of this.tables) {
      this.checkEmptyId(table)
      this.checkDuplicateId(table)
      this.checkParentIdSame(db, table)
      this.checkParentIdNotFound(db, table)
      this.checkForeignId(db, table)
    }
    return this.result
  }

  private checkEmptyId(table: string): void {
    const ids = this.tablesIds[table]
    // If every row yielded an undefined value, we interpret that as: no 'id' field exists at all.
    // In that case the table must NOT be flagged for emptyId.
    if (ids.length > 0 && ids.every(id => id === undefined)) return

    const hasEmptyId = ids.some(
      id => id === null || id === '' || id === undefined,
    )
    if (hasEmptyId) this.result.emptyId.push(table)
  }

  private checkDuplicateId(table: string): void {
    const ids = this.tablesIds[table]
    const set = new Set(ids)
    let duplicates = ids.filter(item => {
      if (set.has(item)) {
        set.delete(item)
      } else {
        return item
      }
    })
    duplicates = Array.from(new Set(duplicates)).filter(
      (id): id is string | number => id != null,
    )
    if (duplicates.length > 0) {
      this.result.duplicateId[table] = duplicates as (string | number)[]
    }
  }

  private checkParentIdSame(
    db: Record<string, TableRow[]>,
    table: string,
  ): void {
    const tableData = db[table] as TableRow[]
    if (tableData.length === 0) return
    if (!Object.keys(tableData[0]).includes('parent' + this.idSuffix)) return

    const parentIdSame: (string | number)[] = []
    for (const row of tableData) {
      if (row.id != null && row.id === row['parent' + this.idSuffix]) {
        parentIdSame.push(row.id)
      }
    }
    if (parentIdSame.length > 0) {
      this.result.parentIdSame[table] = parentIdSame
    }
  }

  private checkParentIdNotFound(
    db: Record<string, TableRow[]>,
    table: string,
  ): void {
    const tableData = db[table] as TableRow[]
    if (tableData.length === 0) return
    if (!Object.keys(tableData[0]).includes('parent' + this.idSuffix)) return

    const parentIdNotFound: (string | number)[] = []
    for (const row of tableData) {
      if (
        row['parent' + this.idSuffix] === null ||
        row['parent' + this.idSuffix] === ''
      )
        continue
      if (
        !this.tablesIds[table].includes(
          row['parent' + this.idSuffix] as string | number,
        )
      ) {
        parentIdNotFound.push(row['parent' + this.idSuffix] as string | number)
      }
    }
    if (parentIdNotFound.length > 0) {
      this.result.parentIdNotFound[table] = parentIdNotFound
    }
  }

  private checkForeignId(db: Record<string, TableRow[]>, table: string): void {
    const tableData = db[table] as TableRow[]
    if (tableData.length === 0) return

    const foreignFields: Array<{ field: string; table: string }> = []
    for (const variable in tableData[0]) {
      if (variable === 'parent' + this.idSuffix) continue
      const relation = resolveRelationField(variable, this.tables)
      if (!relation || relation.many) continue
      foreignFields.push({ field: variable, table: relation.toTable })
    }

    const foreignIdNotFound: Record<string, (string | number)[]> = {}
    for (const foreignField of foreignFields) {
      const foreignTableIdNotFound: (string | number)[] = []

      for (const row of tableData) {
        if (row[foreignField.field] === null || row[foreignField.field] === '')
          continue
        if (
          !this.tablesIds[foreignField.table].includes(
            row[foreignField.field] as string | number,
          )
        ) {
          foreignTableIdNotFound.push(
            row[foreignField.field] as string | number,
          )
        }
      }
      if (foreignTableIdNotFound.length > 0) {
        foreignIdNotFound[foreignField.field] = foreignTableIdNotFound
      }
    }
    if (Object.keys(foreignIdNotFound).length > 0) {
      this.result.foreignIdNotFound[table] = foreignIdNotFound
    }
  }
}
