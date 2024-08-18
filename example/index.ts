import { createHelia } from 'helia';
import { PubSubBroadcaster } from '../lib/pubsub_broadcaster'; // The converted broadcaster
import { Datastore } from '../lib/crdt'; // The CRDT Datastore you converted
import { Key } from 'interface-datastore';
import { MemoryDatastore } from 'datastore-core';

async function runCRDTExample() {
  // Create a libp2p node
  const bc = await PubSubBroadcaster.create('crdt-example-topic');

  // Create a Helia node
  const helia = await createHelia({
    libp2p: bc.libp2p,
  });

  // Create a PubSubBroadcaster
  const broadcaster = new PubSubBroadcaster(helia.libp2p, 'crdt-example-topic');

  // Initialize the CRDT Datastore
  const store1 = new MemoryDatastore();
  const store2 = new MemoryDatastore();
  const crdtStore = new Datastore(store1, new Key('/example-namespace'), helia, broadcaster);

  // Add some data to the CRDT store
  const key = new Key('/example-key');
  const value = new Uint8Array([1, 2, 3, 4, 5]);

  console.log('Putting data into the CRDT store...');
  await crdtStore.put(key, value);

  // Get the data back from the store
  const retrievedValue = await crdtStore.get(key);
  console.log('Retrieved value from the CRDT store:', retrievedValue);

  // Simulate another node by creating a second CRDT store
  const anotherMemoryStore = new Map<string, Uint8Array>();
  const anotherCrdtStore = new Datastore(store2, new Key('/example-namespace'), helia, broadcaster);

  // Simulate receiving the data on the second node
  const anotherRetrievedValue = await anotherCrdtStore.get(key);
  console.log('Retrieved value from the second CRDT store:', anotherRetrievedValue);

  // Ensure both stores have the same data
  console.log('Ensuring both CRDT stores are in sync...');
  const syncResult = JSON.stringify(retrievedValue) === JSON.stringify(anotherRetrievedValue);
  console.log('CRDT stores in sync:', syncResult);

  // Clean up
  await helia.stop();
  // await libp2pNode.stop();
}

runCRDTExample().catch((err) => console.error('Error running CRDT example:', err));
