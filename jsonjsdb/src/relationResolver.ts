export type RelationInfo = {
  field: string
  toTable: string
  role: string | null
  many: boolean
}

export function resolveRelationField(
  field: string,
  tableNames: string[],
): RelationInfo | null {
  const suffix = field.endsWith('Ids')
    ? 'Ids'
    : field.endsWith('Id')
      ? 'Id'
      : null
  if (!suffix) return null

  const baseName = field.slice(0, -suffix.length)
  const exactTable = tableNames.find(tableName => tableName === baseName)
  if (exactTable) {
    return { field, toTable: exactTable, role: null, many: suffix === 'Ids' }
  }

  let matchingTable: string | null = null
  let matchingTableSuffix = ''
  for (const tableName of tableNames) {
    const tableSuffix = capitalize(tableName)
    if (
      tableSuffix.length > matchingTableSuffix.length &&
      baseName.endsWith(tableSuffix)
    ) {
      matchingTable = tableName
      matchingTableSuffix = tableSuffix
    }
  }

  if (!matchingTable) return null

  const role = baseName.slice(0, -matchingTableSuffix.length)
  if (!role) return null

  return { field, toTable: matchingTable, role, many: suffix === 'Ids' }
}

export function relationKeyToField(relationKey: string): string {
  return relationKey.endsWith('Id') ? relationKey : relationKey + 'Id'
}

export function relationFieldToKey(relationField: string): string {
  if (relationField.endsWith('Ids')) return relationField.slice(0, -1)
  if (relationField.endsWith('Id')) return relationField
  return relationField + 'Id'
}

export function relationFieldToReverseKey(relation: RelationInfo): string {
  if (!relation.role) return relation.toTable + 'Id'
  return relation.role + 'Of' + capitalize(relation.toTable) + 'Id'
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1)
}
