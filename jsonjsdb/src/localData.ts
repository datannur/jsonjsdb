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

function createOpenDatabase(
  indexedDB: IDBFactory | undefined,
): () => Promise<IDBDatabase | null> {
  let openDatabasePromise: Promise<IDBDatabase | null> | null = null

  return function openDatabase(): Promise<IDBDatabase | null> {
    if (openDatabasePromise) return openDatabasePromise

    openDatabasePromise = new Promise(resolve => {
      if (!indexedDB) {
        console.error('indexDB not supported')
        resolve(null)
        return
      }

      const request = indexedDB.open(dbName, dbVersion)

      request.onsuccess = () => {
        resolve(request.result)
      }

      request.onerror = event => {
        console.error('indexedDB request error')
        console.log(event)
        resolve(null)
      }

      request.onupgradeneeded = event => {
        const database = (event.target as IDBOpenDBRequest).result
        const store = database.createObjectStore(storeName, { keyPath: 'k' })

        store.transaction.oncomplete = completeEvent => {
          resolve((completeEvent.target as IDBTransaction).db)
        }
      }
    })

    return openDatabasePromise
  }
}

export function createLocalData(indexedDB = globalThis.indexedDB): LocalData {
  const openDatabase = createOpenDatabase(indexedDB)
  void openDatabase()

  function whenReady(action: (db: IDBDatabase) => void): void {
    void openDatabase().then(db => {
      if (db) action(db)
    })
  }

  function readonlyStore(db: IDBDatabase): IDBObjectStore {
    return db.transaction(storeName).objectStore(storeName)
  }

  function readwriteStore(db: IDBDatabase): IDBObjectStore {
    return db.transaction(storeName, 'readwrite').objectStore(storeName)
  }

  return {
    get(key, callback) {
      whenReady(db => {
        readonlyStore(db).get(key).onsuccess = event => {
          const result = (
            event.target as IDBRequest<LocalDataEntry | undefined>
          ).result
          callback(result?.v ?? null)
        }
      })
    },

    set(key, value, callback) {
      whenReady(db => {
        const transaction = db.transaction(storeName, 'readwrite')
        transaction.oncomplete = () => {
          if (callback) callback()
        }
        transaction.objectStore(storeName).put({ k: key, v: value })
        transaction.commit()
      })
    },

    delete(key, callback) {
      whenReady(db => {
        readwriteStore(db).delete(key).onsuccess = () => {
          if (callback) callback()
        }
      })
    },

    list(callback) {
      whenReady(db => {
        readonlyStore(db).getAllKeys().onsuccess = event => {
          callback(
            (event.target as IDBRequest<IDBValidKey[] | null>).result ?? null,
          )
        }
      })
    },

    getAll(callback) {
      whenReady(db => {
        readonlyStore(db).getAll().onsuccess = event => {
          callback(
            (event.target as IDBRequest<LocalDataEntry[] | null>).result ??
              null,
          )
        }
      })
    },

    clear(callback) {
      whenReady(db => {
        readwriteStore(db).clear().onsuccess = () => {
          if (callback) callback()
        }
      })
    },
  }
}

const localData = createLocalData()

if (typeof window !== 'undefined') {
  ;(window as Window & { ldb?: LocalData }).ldb = localData
}

export default localData
