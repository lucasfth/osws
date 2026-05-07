import { KeyClient } from "@azure/keyvault-keys";
import { DefaultAzureCredential } from "@azure/identity";
import Bun from "bun";

const vaultName = Bun.argv[2];
const date = Bun.argv[3];
if (!vaultName) {
  console.error(
    "Missing vault name argument. Usage: bun removeSecretsByDate.ts <vault-name> [YYYY-MM-DD]",
  );
  throw new Error("Missing vault name argument.");
}
const url = `https://${vaultName}.vault.azure.net`;

const credential = new DefaultAzureCredential();
const client = new KeyClient(url, credential);

const today = new Date();
today.setUTCHours(0, 0, 0, 0);

const selectedDate = date ? new Date(date) : today;

const isSameUtcDay = (a: Date, b: Date): boolean => {
  return (
    a.getUTCFullYear() === b.getUTCFullYear() &&
    a.getUTCMonth() === b.getUTCMonth() &&
    a.getUTCDate() === b.getUTCDate()
  );
};

console.log(`Connecting to: ${url}`);
console.log(
  `Checking for keys created on (UTC day): ${selectedDate.toISOString().slice(0, 10)}`,
);

let scanned = 0;
let matched = 0;
let deleted = 0;
let failed = 0;
const requestedConcurrency = Number(Bun.argv[3] ?? 10);
const maxConcurrency =
  Number.isFinite(requestedConcurrency) && requestedConcurrency > 0
    ? Math.floor(requestedConcurrency)
    : 10;

const inFlightDeletes = new Set<Promise<void>>();

for await (const keyProperties of client.listPropertiesOfKeys()) {
  scanned++;
  const createdDate = keyProperties.createdOn;

  if (!createdDate || !isSameUtcDay(createdDate, selectedDate)) {
    console.log(
      `🦘 Skipping key: ${keyProperties.name} (created on ${createdDate})`,
    );
    continue;
  }

  matched++;
  const deleteTask = (async () => {
    try {
      console.log(`ℹ️ Deleting key: ${keyProperties.name}...`);
      const poller = await client.beginDeleteKey(keyProperties.name);
      await poller.pollUntilDone();
      deleted++;
      console.log(`✅ Deleted: ${keyProperties.name}`);
    } catch (error) {
      failed++;
      console.error(`❌ Failed to delete key: ${keyProperties.name}`, error);
    }
  })();

  inFlightDeletes.add(deleteTask);
  deleteTask.finally(() => inFlightDeletes.delete(deleteTask));

  if (inFlightDeletes.size >= maxConcurrency) {
    await Promise.race(inFlightDeletes);
  }
}

await Promise.all(inFlightDeletes);

console.log(
  `Cleanup complete. Scanned ${scanned} keys, matched ${matched}, deleted ${deleted}, failed ${failed}.`,
);
