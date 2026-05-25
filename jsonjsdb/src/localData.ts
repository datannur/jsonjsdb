type LocalDataEntry = {
  k: string
  v: unknown
}

export type LocalData = {
  get(key: string, callback: (data: unknown) => void): void
  set(key: string, data: unknown, callback?: () => void): void
  delete(key: string, callback?: () => void): void
  list(callback: (keys: IDBValidKey[] | null) => void): void
  getAll(callback: (entries: LocalDataEntry[] | null) => void): void
  clear(callback?: () => void): void
}

const dbName = 'ldb'
const dbVersion = 1
const storeName = 's'
const readyRetryDelayMs = 50

let db: IDBDatabase | null = null
let unavailable = false

function openDatabase(): void {
  const indexedDB = globalThis.indexedDB
  if (!indexedDB) {
    unavailable = true
    console.error('indexDB not supported')
    return
  }

  const request = indexedDB.open(dbName, dbVersion)

  request.onsuccess = function () {
    db = this.result
  }

  request.onerror = event => {
    unavailable = true
    console.error('indexedDB request error')
    console.log(event)
  }

  request.onupgradeneeded = event => {
    db = null
    const database = (event.target as IDBOpenDBRequest).result
    const store = database.createObjectStore(storeName, { keyPath: 'k' })

    store.transaction.oncomplete = completeEvent => {
      db = (completeEvent.target as IDBTransaction).db
    }
  }
}

function whenReady(action: () => void): void {
  if (db) {
    action()
    return
  }

  if (unavailable) return

  setTimeout(() => whenReady(action), readyRetryDelayMs)
}

function readonlyStore(): IDBObjectStore {
  return db!.transaction(storeName).objectStore(storeName)
}

function readwriteStore(): IDBObjectStore {
  return db!.transaction(storeName, 'readwrite').objectStore(storeName)
}

openDatabase()

const localData: LocalData = {
  get(key, callback) {
    whenReady(() => {
      readonlyStore().get(key).onsuccess = event => {
        const result = (event.target as IDBRequest<LocalDataEntry | undefined>)
          .result
        callback(result?.v ?? null)
      }
    })
  },

  set(key, value, callback) {
    whenReady(() => {
      const transaction = db!.transaction(storeName, 'readwrite')
      transaction.oncomplete = () => {
        if (callback) callback()
      }
      transaction.objectStore(storeName).put({ k: key, v: value })
      transaction.commit()
    })
  },

  delete(key, callback) {
    whenReady(() => {
      readwriteStore().delete(key).onsuccess = () => {
        if (callback) callback()
      }
    })
  },

  list(callback) {
    whenReady(() => {
      readonlyStore().getAllKeys().onsuccess = event => {
        callback(
          (event.target as IDBRequest<IDBValidKey[] | null>).result ?? null,
        )
      }
    })
  },

  getAll(callback) {
    whenReady(() => {
      readonlyStore().getAll().onsuccess = event => {
        callback(
          (event.target as IDBRequest<LocalDataEntry[] | null>).result ?? null,
        )
      }
    })
  },

  clear(callback) {
    whenReady(() => {
      readwriteStore().clear().onsuccess = () => {
        if (callback) callback()
      }
    })
  },
}

if (typeof window !== 'undefined') {
  ;(window as Window & { ldb?: LocalData }).ldb = localData
}

export default localData
