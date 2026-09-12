/**
 * Minimal 5-field cron expression parser (minute hour day-of-month month day-of-week).
 * Numeric fields only; evaluated in the server's local time. Supports `*`,
 * single values, ranges like `a-b`, lists like `a,b,c`, and step values
 * like `STAR/n` (any-n) and `a-b/n` (range-n).
 */

const FIELD_MIN = [0, 0, 1, 1, 0] as const
// Day-of-week accepts 0-7 (both 0 and 7 are Sunday); parseCron normalizes 7 to 0.
const FIELD_MAX = [59, 23, 31, 12, 7] as const
const FIELD_NAMES = ["minute", "hour", "day-of-month", "month", "day-of-week"] as const

export type CronFields = {
  minute: number[]
  hour: number[]
  dayOfMonth: number[]
  month: number[]
  dayOfWeek: number[]
  domStar: boolean
  dowStar: boolean
}

function parseValue(raw: string, min: number, max: number, field: string): number {
  if (!/^\d+$/.test(raw)) throw new Error(`Invalid ${field} value "${raw}" in cron expression`)
  const value = Number(raw)
  if (value < min || value > max) {
    throw new Error(`${field} value ${value} out of range (${min}-${max})`)
  }
  return value
}

function parseField(raw: string, index: number): number[] {
  const field = FIELD_NAMES[index]!
  const min = FIELD_MIN[index]!
  const max = FIELD_MAX[index]!
  const values = new Set<number>()
  for (const part of raw.split(",")) {
    const slash = part.indexOf("/")
    const range = slash === -1 ? part : part.slice(0, slash)
    const stepRaw = slash === -1 ? undefined : part.slice(slash + 1)
    const step = stepRaw === undefined ? 1 : parseValue(stepRaw, 1, max, field)
    let lo: number
    let hi: number
    if (range === "*") {
      lo = min
      hi = max
    } else if (range.includes("-")) {
      const dash = range.indexOf("-")
      lo = parseValue(range.slice(0, dash), min, max, field)
      hi = parseValue(range.slice(dash + 1), min, max, field)
      if (lo > hi) throw new Error(`Invalid range "${range}" in cron ${field} field`)
    } else {
      lo = parseValue(range, min, max, field)
      hi = stepRaw === undefined ? lo : max
    }
    for (let value = lo; value <= hi; value += step) values.add(value)
  }
  if (values.size === 0) throw new Error(`Empty cron ${field} field`)
  return [...values].sort((a, b) => a - b)
}

export function parseCron(expr: string): CronFields {
  const fields = expr.trim().split(/\s+/)
  if (fields.length !== 5) {
    throw new Error(`Cron expression must have 5 fields (minute hour day-of-month month day-of-week), got ${fields.length}`)
  }
  const minute = parseField(fields[0]!, 0)
  const hour = parseField(fields[1]!, 1)
  const dayOfMonth = parseField(fields[2]!, 2)
  const month = parseField(fields[3]!, 3)
  // 0 and 7 both mean Sunday; normalize 7 to 0.
  const dayOfWeek = [...new Set(parseField(fields[4]!, 4).map((value) => (value === 7 ? 0 : value)))].sort((a, b) => a - b)
  return {
    minute,
    hour,
    dayOfMonth,
    month,
    dayOfWeek,
    domStar: fields[2] === "*",
    dowStar: fields[4] === "*",
  }
}

function dayMatches(fields: CronFields, date: Date): boolean {
  const domOk = fields.dayOfMonth.includes(date.getDate())
  const dowOk = fields.dayOfWeek.includes(date.getDay())
  // Standard cron: when both day fields are restricted, either may match.
  if (!fields.domStar && !fields.dowStar) return domOk || dowOk
  return (fields.domStar || domOk) && (fields.dowStar || dowOk)
}

function nextValue(values: number[], current: number): number | undefined {
  return values.find((value) => value > current)
}

/** Next minute strictly after `from` that matches the expression, in local time. */
export function nextRun(expr: string, from: Date): Date {
  const fields = parseCron(expr)
  const result = new Date(from)
  result.setSeconds(0, 0)
  result.setMinutes(result.getMinutes() + 1)
  // A valid expression always fires within 4 years (leap-day rules); the guard
  // only bounds pathological inputs.
  for (let guard = 0; guard < 3000; guard++) {
    if (!fields.month.includes(result.getMonth() + 1)) {
      result.setDate(1)
      result.setMonth(result.getMonth() + 1)
      result.setHours(0, 0, 0, 0)
      continue
    }
    if (!dayMatches(fields, result)) {
      result.setDate(result.getDate() + 1)
      result.setHours(0, 0, 0, 0)
      continue
    }
    if (!fields.hour.includes(result.getHours())) {
      const hour = nextValue(fields.hour, result.getHours())
      if (hour === undefined) {
        result.setDate(result.getDate() + 1)
        result.setHours(0, 0, 0, 0)
      } else {
        result.setHours(hour, 0, 0, 0)
      }
      continue
    }
    if (!fields.minute.includes(result.getMinutes())) {
      const minute = nextValue(fields.minute, result.getMinutes())
      if (minute === undefined) {
        result.setHours(result.getHours() + 1, 0, 0, 0)
      } else {
        result.setMinutes(minute, 0, 0)
      }
      continue
    }
    return result
  }
  throw new Error(`Cron expression "${expr}" never fires`)
}

/** Whether `date` matches the expression, to the minute. */
export function matchesCron(expr: string, date: Date): boolean {
  const fields = parseCron(expr)
  return (
    fields.minute.includes(date.getMinutes()) &&
    fields.hour.includes(date.getHours()) &&
    fields.month.includes(date.getMonth() + 1) &&
    dayMatches(fields, date)
  )
}

/** "HH:MM" (24-hour, local time) -> the next time that hour:minute occurs, strictly after `from`. */
export function nextDailyAt(at: string, from: Date): Date {
  const match = /^(\d{1,2}):(\d{2})$/.exec(at.trim())
  const hour = match ? Number(match[1]) : NaN
  const minute = match ? Number(match[2]) : NaN
  if (!match || !(hour >= 0 && hour < 24) || !(minute >= 0 && minute < 60)) {
    throw new Error(`daily_at must be "HH:MM" in 24-hour local time, got: ${at}`)
  }
  const result = new Date(from)
  result.setHours(hour, minute, 0, 0)
  if (result.getTime() <= from.getTime()) result.setDate(result.getDate() + 1)
  return result
}
