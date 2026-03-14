import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import JavaScriptObfuscator from "javascript-obfuscator";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.resolve(__dirname, "..", "dist");

function walk(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...walk(full));
    else files.push(full);
  }
  return files;
}

const files = walk(distDir).filter((f) => f.endsWith(".js"));
for (const file of files) {
  const code = fs.readFileSync(file, "utf8");
  const strong = process.env.OBFUSCATE_STRONG === "1";
  const obfuscated = JavaScriptObfuscator.obfuscate(code, {
    compact: true,
    simplify: true,
    controlFlowFlattening: strong,
    controlFlowFlatteningThreshold: strong ? 0.75 : 0.3,
    deadCodeInjection: strong,
    deadCodeInjectionThreshold: strong ? 0.35 : 0.1,
    stringArray: true,
    stringArrayEncoding: ["base64"],
    stringArrayThreshold: strong ? 0.8 : 0.6,
    stringArrayRotate: true,
    stringArrayShuffle: true,
    stringArrayWrappersCount: strong ? 3 : 1,
    stringArrayWrappersType: "function",
    stringArrayWrappersChainedCalls: true,
    identifierNamesGenerator: "hexadecimal",
    numbersToExpressions: strong,
    splitStrings: strong,
    splitStringsChunkLength: 6,
    unicodeEscapeSequence: false,
  }).getObfuscatedCode();
  fs.writeFileSync(file, obfuscated, "utf8");
}

console.log(`Obfuscated ${files.length} JS bundle(s).`);
