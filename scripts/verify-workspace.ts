import { existsSync, readFileSync } from "node:fs";

const required = [
  "Cargo.toml",
  "rust-toolchain.toml",
  "package.json",
  "bunfig.toml",
  ".env.example",
  "apps/web/package.json",
];

const expectedCrates = [
  "erabi-domain",
  "erabi-db",
  "erabi-api",
  "erabi-jobs",
  "erabi-crawler",
  "erabi-crawl4ai",
  "erabi-extraction",
  "erabi-export",
  "erabi-cli",
];

for (const path of required) {
  if (!existsSync(path)) throw new Error(`missing ${path}`);
}

for (const crate of expectedCrates) {
  if (!existsSync(`crates/${crate}/Cargo.toml`)) {
    throw new Error(`missing Cargo workspace member ${crate}`);
  }
}

const pkg = JSON.parse(readFileSync("package.json", "utf8"));
if (!Array.isArray(pkg.workspaces) || !pkg.workspaces.includes("apps/*")) {
  throw new Error("apps/* Bun workspace is required");
}
if (typeof pkg.packageManager !== "string" || !pkg.packageManager.startsWith("bun@")) {
  throw new Error("packageManager must record the executing Bun version");
}

for (const unsupportedLockfile of ["package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock"]) {
  if (existsSync(unsupportedLockfile)) {
    throw new Error(`unsupported JavaScript lockfile ${unsupportedLockfile}`);
  }
}

console.log("workspace contract ok");
