import { KeyClient } from "@azure/keyvault-keys";
import { DefaultAzureCredential } from "@azure/identity";
import Bun from "bun";

const vaultName = Bun.argv[2];
if (!vaultName) {
  console.error(
    "Missing vault name argument. Usage: bun listKeys.ts <vault-name>",
  );
  throw new Error("Missing vault name argument.");
}
const url = `https://${vaultName}.vault.azure.net`;

const credential = new DefaultAzureCredential();
const client = new KeyClient(url, credential);

let scanned = 0;

const keys: string[] = [];

console.log(`Connecting to: ${url}`);
console.log(`Listing keys...`);

for await (const keyProperties of client.listPropertiesOfKeys()) {
  keys.push(`Created on ${keyProperties.createdOn}\t${keyProperties.name}`);
  scanned++;
}

// Sort by createdOn date, oldest to newest
keys.sort((a, b) => {
  const dateA = new Date(a.split("\t")[0]);
  const dateB = new Date(b.split("\t")[0]);

  return dateA.getTime() - dateB.getTime();
});

console.log(`\nFound ${scanned} keys:`);
for (const key of keys) {
  console.log(key);
}
