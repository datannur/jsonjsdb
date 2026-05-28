import { describe, expect, it } from 'vitest'
import { createLocalData } from '../src/localData'

type OpenRequest = IDBOpenDBRequest & {
  triggerSuccess(): void
  triggerUpgrade(): IDBTransaction
  triggerError(): void
}

class FakeObjectStore {
  constructor(
    private readonly values: Map<string, unknown>,
    readonly transaction: IDBTransaction,
  ) {}

  get(key: IDBValidKey): IDBRequest<unknown> {
    const entry = this.values.get(String(key))
    return this.successRequest(entry)
  }

  put(entry: { k: string; v: unknown }): IDBRequest<IDBValidKey> {
    this.values.set(entry.k, entry)
    return this.successRequest<IDBValidKey>(entry.k)
  }

  delete(key: IDBValidKey): IDBRequest<undefined> {
    this.values.delete(String(key))
    return this.successRequest(undefined)
  }

  getAllKeys(): IDBRequest<IDBValidKey[]> {
    return this.successRequest<IDBValidKey[]>([...this.values.keys()])
  }

  getAll(): IDBRequest<unknown[]> {
    return this.successRequest([...this.values.values()])
  }

  clear(): IDBRequest<undefined> {
    this.values.clear()
    return this.successRequest(undefined)
  }

  private successRequest<T>(result: T): IDBRequest<T> {
    const request = {
      result,
      onsuccess: null,
    } as IDBRequest<T>

    queueMicrotask(() => {
      request.onsuccess?.({ target: request } as unknown as Event)
    })

    return request
  }
}

class FakeTransaction {
  oncomplete: ((event: Event) => void) | null = null

  constructor(
    readonly db: IDBDatabase,
    private readonly values: Map<string, unknown>,
  ) {}

  objectStore(): IDBObjectStore {
    return new FakeObjectStore(
      this.values,
      this as unknown as IDBTransaction,
    ) as unknown as IDBObjectStore
  }

  commit(): void {
    queueMicrotask(() => {
      this.oncomplete?.({ target: this } as unknown as Event)
    })
  }

  triggerComplete(): void {
    this.oncomplete?.({ target: this } as unknown as Event)
  }
}

class FakeDatabase {
  readonly values = new Map<string, unknown>()
  upgradeTransaction: FakeTransaction | null = null

  transaction(): IDBTransaction {
    return new FakeTransaction(
      this as unknown as IDBDatabase,
      this.values,
    ) as unknown as IDBTransaction
  }

  createObjectStore(): IDBObjectStore {
    const transaction = new FakeTransaction(
      this as unknown as IDBDatabase,
      this.values,
    )
    this.upgradeTransaction = transaction
    return new FakeObjectStore(
      this.values,
      transaction as unknown as IDBTransaction,
    ) as unknown as IDBObjectStore
  }
}

function createFakeIndexedDB() {
  const database = new FakeDatabase()
  let openRequest: OpenRequest | null = null
  let openCount = 0

  const indexedDB = {
    open(): IDBOpenDBRequest {
      openCount += 1
      const request = {
        result: database,
        onsuccess: null,
        onerror: null,
        onupgradeneeded: null,
        triggerSuccess() {
          request.onsuccess?.({ target: request } as unknown as Event)
        },
        triggerUpgrade() {
          request.onupgradeneeded?.({
            target: request,
          } as unknown as IDBVersionChangeEvent)
          if (!database.upgradeTransaction) {
            throw new Error('Upgrade transaction was not created')
          }
          return database.upgradeTransaction as unknown as IDBTransaction
        },
        triggerError() {
          request.onerror?.({ target: request } as unknown as Event)
        },
      } as unknown as OpenRequest
      openRequest = request
      return request
    },
  } as unknown as IDBFactory

  return {
    indexedDB,
    database,
    get openRequest() {
      if (!openRequest) throw new Error('IndexedDB was not opened')
      return openRequest
    },
    get openCount() {
      return openCount
    },
  }
}

describe('localData readiness', () => {
  it('opens IndexedDB when localData is created', () => {
    const fake = createFakeIndexedDB()

    createLocalData(fake.indexedDB)

    expect(fake.openCount).toBe(1)
  })

  it('shares one IndexedDB open request for concurrent first operations', async () => {
    const fake = createFakeIndexedDB()
    const localData = createLocalData(fake.indexedDB)
    const setCallbacks: string[] = []
    const results: unknown[] = []

    localData.set('first', 'one', () => setCallbacks.push('first'))
    localData.set('second', 'two', () => setCallbacks.push('second'))
    localData.get('first', result => results.push(result))

    expect(fake.openCount).toBe(1)
    expect(setCallbacks).toEqual([])
    expect(results).toEqual([])

    fake.openRequest.triggerSuccess()
    await Promise.resolve()
    await Promise.resolve()

    expect(setCallbacks).toEqual(['first', 'second'])
    expect(results).toEqual(['one'])
  })

  it('waits for the upgrade transaction before running queued operations', async () => {
    const fake = createFakeIndexedDB()
    const localData = createLocalData(fake.indexedDB)
    const setCallbacks: string[] = []

    localData.set('upgraded', true, () => setCallbacks.push('upgraded'))

    const upgradeTransaction =
      fake.openRequest.triggerUpgrade() as unknown as FakeTransaction
    await Promise.resolve()
    await Promise.resolve()

    expect(setCallbacks).toEqual([])

    upgradeTransaction.triggerComplete()
    await Promise.resolve()
    await Promise.resolve()

    expect(setCallbacks).toEqual(['upgraded'])
  })

  it('does not run queued operations when IndexedDB is unavailable', async () => {
    const localData = createLocalData(undefined)
    const results: unknown[] = []

    localData.get('missing', result => results.push(result))
    await Promise.resolve()

    expect(results).toEqual([])
  })
})
