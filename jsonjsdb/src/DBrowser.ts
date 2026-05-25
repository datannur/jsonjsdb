import ldb, { type LocalData } from './localData'

type LdbEntry = {
  k: string
  v: unknown
}

export default class DBrowser {
  private ldb: LocalData
  private namespaced: (key: string) => string

  constructor(appName: string) {
    this.ldb = ldb
    this.namespaced = key => appName + '/' + key
  }

  getAll(key: string, callback: (data: Record<string, unknown>) => void): void {
    const prefix = this.namespaced(key)
    const data: Record<string, unknown> = {}
    this.ldb.getAll((entries: unknown) => {
      const entriesArray = entries as LdbEntry[]
      for (const entry of entriesArray) {
        if (!entry.k.startsWith(prefix)) continue
        const keySuffix = entry.k.substring(prefix.length)
        if (keySuffix) {
          data[keySuffix] = entry.v
        }
      }
      callback(data)
    })
  }

  get(key: string): Promise<unknown> {
    return new Promise(resolve => {
      this.ldb.get(this.namespaced(key), resolve)
    })
  }

  set(key: string, data: unknown, callback?: () => void): void {
    this.ldb.set(this.namespaced(key), data, callback)
  }

  clear(): void {
    this.ldb.clear()
  }
}
