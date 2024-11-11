/* import { Key } from "interface-datastore"

const versionKey = 'crdt_version'

// Use this to detect if we need to run migrations.
const version = 1

function getVersionKey (namespace: Key): Key {
  return namespace.child(new Key(versionKey))
}

function getVersion() {
  const versionK = getVersionKey()
  data, err := store.store.Get(ctx, versionK)
  if err != nil {
    if err == ds.ErrNotFound {
      return 0, nil
    }
    return 0, err
  }

  v, n := binary.Uvarint(data)
  if n <= 0 {
    return v, errors.New("error decoding version")
  }
  return v - 1, nil
} */
