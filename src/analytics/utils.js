import { createHash } from "node:crypto"

export function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(",")}]`
  const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right))
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`).join(",")}}`
}

export function hashPayload(value) {
  return createHash("sha1").update(stableStringify(value)).digest("hex")
}

export function textValue(value) {
  if (value === null || value === undefined) return null
  return typeof value === "string" ? value : JSON.stringify(value)
}

export function byteLength(value) {
  const text = textValue(value)
  return text ? Buffer.byteLength(text, "utf8") : 0
}

export function dedupeBy(items, keyFn) {
  const result = new Map()
  for (const item of items) {
    result.set(keyFn(item), item)
  }
  return Array.from(result.values())
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export function toErrorMessage(error) {
  if (error instanceof Error) return error.message
  return String(error)
}

export function compact(array) {
  return array.filter(Boolean)
}

export function coalesce(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined) return value
  }
  return null
}

export function chunk(items, size) {
  const result = []
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size))
  }
  return result
}

export function sumCounts(counts) {
  return Object.values(counts).reduce((total, value) => total + value, 0)
}
