import { describe, expect, it } from "vitest"
import { matchesCron, nextDailyAt, nextRun, parseCron } from "../src/cron.js"

describe("parseCron", () => {
  it("parses a valid expression", () => {
    const fields = parseCron("0 9 1,15 * 1-5")
    expect(fields.minute).toEqual([0])
    expect(fields.hour).toEqual([9])
    expect(fields.dayOfMonth).toEqual([1, 15])
    expect(fields.month).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    expect(fields.dayOfWeek).toEqual([1, 2, 3, 4, 5])
    expect(fields.domStar).toBe(false)
    expect(fields.dowStar).toBe(false)
  })

  it("normalizes day-of-week 7 to Sunday", () => {
    expect(parseCron("0 0 * * 7").dayOfWeek).toEqual([0])
  })

  it("marks star day fields", () => {
    const fields = parseCron("*/15 * * * *")
    expect(fields.domStar).toBe(true)
    expect(fields.dowStar).toBe(true)
  })

  it("rejects wrong field counts", () => {
    expect(() => parseCron("* * * *")).toThrow("5 fields")
    expect(() => parseCron("* * * * * *")).toThrow("5 fields")
  })

  it("rejects out-of-range and malformed values", () => {
    expect(() => parseCron("60 * * * *")).toThrow("minute")
    expect(() => parseCron("* 24 * * *")).toThrow("hour")
    expect(() => parseCron("* * 0 * *")).toThrow("day-of-month")
    expect(() => parseCron("* * * 13 *")).toThrow("month")
    expect(() => parseCron("* * * * 8")).toThrow("day-of-week")
    expect(() => parseCron("a * * * *")).toThrow("minute")
    expect(() => parseCron("10-5 * * * *")).toThrow("range")
    expect(() => parseCron("*/0 * * * *")).toThrow("minute")
  })
})

describe("nextRun", () => {
  it("returns the next minute for every-minute schedules", () => {
    const from = new Date(2026, 8, 11, 10, 30, 15)
    expect(nextRun("* * * * *", from)).toEqual(new Date(2026, 8, 11, 10, 31))
  })

  it("returns the next daily occurrence", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(nextRun("0 9 * * *", from)).toEqual(new Date(2026, 8, 12, 9, 0))
  })

  it("returns the same day when the time is still ahead", () => {
    const from = new Date(2026, 8, 11, 8, 0, 0)
    expect(nextRun("0 9 * * *", from)).toEqual(new Date(2026, 8, 11, 9, 0))
  })

  it("skips to the next matching day of month", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(nextRun("30 14 1 * *", from)).toEqual(new Date(2026, 9, 1, 14, 30))
  })

  it("advances to the next matching month", () => {
    const from = new Date(2026, 1, 10, 0, 0, 0)
    expect(nextRun("0 0 1 6 *", from)).toEqual(new Date(2026, 5, 1, 0, 0))
  })

  it("finds the next weekday", () => {
    // 2026-09-11 is a Friday.
    const from = new Date(2026, 8, 11, 12, 0, 0)
    expect(nextRun("0 0 * * 1-5", from)).toEqual(new Date(2026, 8, 14, 0, 0))
  })

  it("treats restricted day-of-month and day-of-week as OR", () => {
    // Fires on the 13th or on Fridays, whichever comes first.
    const from = new Date(2026, 8, 14, 12, 0, 0) // Monday 2026-09-14
    expect(nextRun("0 0 13 * 5", from)).toEqual(new Date(2026, 8, 18, 0, 0)) // Friday
  })

  it("supports step values", () => {
    const from = new Date(2026, 8, 11, 10, 7, 0)
    expect(nextRun("*/15 * * * *", from)).toEqual(new Date(2026, 8, 11, 10, 15))
  })

  it("supports ranges with steps", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(nextRun("10-50/20 * * * *", from)).toEqual(new Date(2026, 8, 11, 10, 10))
  })

  it("rolls over year boundaries", () => {
    const from = new Date(2026, 11, 31, 23, 30, 0)
    expect(nextRun("0 0 1 1 *", from)).toEqual(new Date(2027, 0, 1, 0, 0))
  })

  it("handles minute wrap within an hour", () => {
    const from = new Date(2026, 8, 11, 10, 45, 0)
    expect(nextRun("5,10 * * * *", from)).toEqual(new Date(2026, 8, 11, 11, 5))
  })
})

describe("nextDailyAt", () => {
  it("returns the same day when the time is still ahead", () => {
    const from = new Date(2026, 8, 11, 8, 0, 0)
    expect(nextDailyAt("09:30", from)).toEqual(new Date(2026, 8, 11, 9, 30))
  })

  it("rolls to tomorrow when the time has passed", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(nextDailyAt("09:30", from)).toEqual(new Date(2026, 8, 12, 9, 30))
  })

  it("accepts midnight and single-digit hours", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(nextDailyAt("0:05", from)).toEqual(new Date(2026, 8, 12, 0, 5))
  })

  it("rejects malformed values", () => {
    const from = new Date(2026, 8, 11, 10, 0, 0)
    expect(() => nextDailyAt("24:00", from)).toThrow("HH:MM")
    expect(() => nextDailyAt("9:60", from)).toThrow("HH:MM")
    expect(() => nextDailyAt("nine", from)).toThrow("HH:MM")
    expect(() => nextDailyAt("9", from)).toThrow("HH:MM")
  })
})

describe("matchesCron", () => {
  it("matches exact minutes", () => {
    expect(matchesCron("0 9 * * *", new Date(2026, 8, 11, 9, 0))).toBe(true)
    expect(matchesCron("0 9 * * *", new Date(2026, 8, 11, 9, 1))).toBe(false)
    expect(matchesCron("*/15 * * * *", new Date(2026, 8, 11, 10, 45))).toBe(true)
    expect(matchesCron("*/15 * * * *", new Date(2026, 8, 11, 10, 46))).toBe(false)
  })
})
