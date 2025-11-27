// index.js - Gold Price Monitor v2.1 with Redis
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  Browsers,
  makeCacheableSignalKeyStore
} from '@whiskeysockets/baileys'
import pino from 'pino'
import express from 'express'
import http from 'http'
import https from 'https'
import { Redis } from '@upstash/redis'

// Redis untuk persistent storage
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || 'https://robust-mole-31555.upstash.io',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || 'AXtDAAIncDIxOWMyMWMzYjQ0MjI0MzJlYWQwNTRkMzM0MjgxYWIxNXAyMzE1NTU'
})

// HTTP Keep-Alive agents untuk koneksi lebih cepat
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 10 })
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 10 })

// ------ CONFIG ------
const PORT = process.env.PORT || 8000
const TREASURY_URL = process.env.TREASURY_URL ||
  'https://api.treasury.id/api/v1/antigrvty/gold/rate'

// Anti-spam settings
const COOLDOWN_PER_CHAT = 60000
const GLOBAL_THROTTLE = 3000
const TYPING_DURATION = 2000

// BROADCAST COOLDOWN
const PRICE_CHECK_INTERVAL = 500 // 500ms - balanced speed
const MIN_PRICE_CHANGE = 1
const BROADCAST_COOLDOWN = 50000 // 50 detik antar broadcast (atau ganti menit)

// Economic Calendar Settings
const ECONOMIC_CALENDAR_ENABLED = true
const CALENDAR_COUNTRY_FILTER = ['USD']
const CALENDAR_MIN_IMPACT = 3

// Broadcast Settings
const BATCH_SIZE = 20 // Max messages per batch
const BATCH_DELAY = 1000 // Delay between batches (ms)

// Konversi troy ounce ke gram
const TROY_OZ_TO_GRAM = 31.1034768

// Threshold untuk harga normal/abnormal
const NORMAL_THRESHOLD = 2000
const NORMAL_LOW_THRESHOLD = 1000

// Cache untuk XAU/USD
let cachedXAUUSD = null
let lastXAUUSDFetch = 0
const XAU_CACHE_DURATION = 30000

// History untuk chart XAU/USD (simpan 60 data points = 30 menit dengan interval 30 detik)
const xauHistory = []
const MAX_XAU_HISTORY = 60

// Cache untuk Economic Calendar
let cachedEconomicEvents = null
let lastEconomicFetch = 0
const ECONOMIC_CACHE_DURATION = 300000 // 5 menit

let lastKnownPrice = null
let lastBroadcastedPrice = null
let isBroadcasting = false
let broadcastCount = 0
let lastBroadcastTime = 0
let lastBroadcastMinute = -1  // Track menit terakhir broadcast untuk hindari 2x di menit sama
let lastBroadcastMessage = ''  // Simpan pesan terakhir untuk monitoring

// ⏱️ STALE PRICE DETECTION
let lastPriceUpdateTime = 0  // Kapan terakhir harga berubah dari API
const STALE_PRICE_THRESHOLD = 5 * 60 * 1000  // 5 menit

// Reconnect settings
let reconnectAttempts = 0
const MAX_RECONNECT_ATTEMPTS = 10
const BASE_RECONNECT_DELAY = 5000

// ------ STATE ------
let lastQr = null
const logs = []
const processedMsgIds = new Set()
const lastReplyAtPerChat = new Map()
let lastGlobalReplyAt = 0
let isReady = false
let sock = null

const subscriptions = new Set()

// CACHE GLOBAL untuk market data (pre-fetched)
let cachedMarketData = {
  usdIdr: { rate: 16600 }, // Updated default to current market rate
  xauUsd: null,
  economicEvents: null,
  lastUpdate: 0,
  lastUsdIdrFetch: 0 // Track kapan terakhir fetch USD/IDR
}

// ==================== REDIS STORAGE ====================
const REDIS_KEYS = {
  DAILY_STATS: 'gold:daily_stats',
  PRICE_HISTORY: 'gold:price_history'
}

// Cache lokal untuk mengurangi Redis calls
let dailyStatsCache = null
let priceHistoryCache = []
let lastCacheUpdate = 0
const CACHE_TTL = 5000 // 5 detik

// Load data dari Redis saat startup
async function loadFromRedis() {
  try {
    const [stats, history] = await Promise.all([
      redis.get(REDIS_KEYS.DAILY_STATS),
      redis.lrange(REDIS_KEYS.PRICE_HISTORY, 0, -1)
    ])

    if (stats) {
      dailyStatsCache = stats
      pushLog('REDIS | Daily stats loaded')
    }

    if (history && history.length > 0) {
      priceHistoryCache = history
      pushLog(`REDIS | ${history.length} price history loaded`)
    }
  } catch (e) {
    pushLog('REDIS | Load error: ' + e.message)
  }
}

// Update daily stats ke Redis
async function updateDailyStats(buyPrice) {
  const now = new Date()
  // Konversi ke WIB
  const wibOffset = 7 * 60 * 60 * 1000
  const wibTime = new Date(now.getTime() + wibOffset + now.getTimezoneOffset() * 60 * 1000)
  const today = wibTime.toISOString().split('T')[0]

  try {
    let stats = dailyStatsCache || await redis.get(REDIS_KEYS.DAILY_STATS)

    // Reset jika hari baru
    if (!stats || stats.date !== today) {
      stats = {
        date: today,
        open: buyPrice,
        high: buyPrice,
        low: buyPrice,
        prices: [buyPrice],
        lastUpdate: Date.now()
      }
    } else {
      // Update stats
      if (stats.open === null) stats.open = buyPrice
      if (buyPrice > stats.high) stats.high = buyPrice
      if (buyPrice < stats.low) stats.low = buyPrice

      // Simpan harga untuk average (max 500 untuk Redis)
      if (!stats.prices) stats.prices = []
      if (stats.prices.length < 500) {
        stats.prices.push(buyPrice)
      } else {
        stats.prices.shift()
        stats.prices.push(buyPrice)
      }
      stats.lastUpdate = Date.now()
    }

    // Simpan ke Redis dan cache
    await redis.set(REDIS_KEYS.DAILY_STATS, stats)
    dailyStatsCache = stats
  } catch (e) {
    pushLog('REDIS | Update daily stats error: ' + e.message)
  }
}

// Get daily stats
async function getDailyStats() {
  try {
    // Gunakan cache jika masih fresh
    if (dailyStatsCache && Date.now() - lastCacheUpdate < CACHE_TTL) {
      return formatDailyStats(dailyStatsCache)
    }

    const stats = await redis.get(REDIS_KEYS.DAILY_STATS)
    if (stats) {
      dailyStatsCache = stats
      lastCacheUpdate = Date.now()
      return formatDailyStats(stats)
    }
  } catch (e) {}

  return { open: null, high: null, low: null, avg: null, change: null, changePct: null }
}

function formatDailyStats(stats) {
  if (!stats || !stats.date || !stats.prices || stats.prices.length === 0) {
    return { open: null, high: null, low: null, avg: null, change: null, changePct: null }
  }

  const avg = Math.round(stats.prices.reduce((a, b) => a + b, 0) / stats.prices.length)
  const current = stats.prices[stats.prices.length - 1]
  const change = current - stats.open
  const changePct = ((change / stats.open) * 100).toFixed(2)

  return {
    date: stats.date,
    open: stats.open,
    high: stats.high,
    low: stats.low,
    avg: avg,
    current: current,
    change: change,
    changePct: changePct
  }
}

// Add price history ke Redis
let isAddingHistory = false // Lock untuk mencegah race condition
let lastAddedUpdatedAt = '' // Track updatedAt terakhir yang sudah ditambahkan

async function addPriceHistory(buy, sell, prevBuy, prevSell, updatedAt) {
  // Skip jika updatedAt sama dengan yang terakhir ditambahkan
  if (updatedAt === lastAddedUpdatedAt) return

  // Cek lock untuk mencegah race condition
  if (isAddingHistory) return
  isAddingHistory = true

  try {
    // Cek apakah sudah ada entry dengan updatedAt yang sama
    const lastEntry = priceHistoryCache[priceHistoryCache.length - 1]
    if (lastEntry && lastEntry.time === updatedAt) {
      isAddingHistory = false
      return
    }

    const entry = {
      time: updatedAt, // Pakai waktu dari API Treasury
      buy: buy,
      sell: sell,
      buyChange: buy - prevBuy,
      sellChange: sell - prevSell
    }

    // Tambah entry baru
    await redis.rpush(REDIS_KEYS.PRICE_HISTORY, entry)
    priceHistoryCache.push(entry)
    lastAddedUpdatedAt = updatedAt

    // Limit max 1440 entries (24 jam)
    const len = await redis.llen(REDIS_KEYS.PRICE_HISTORY)
    if (len > 1440) {
      await redis.lpop(REDIS_KEYS.PRICE_HISTORY)
      priceHistoryCache.shift()
    }
  } catch (e) {
    pushLog('REDIS | Add history error: ' + e.message)
  } finally {
    isAddingHistory = false
  }
}

// Get price history dengan pagination
async function getPriceHistory(page = 1, perPage = 10) {
  try {
    const total = await redis.llen(REDIS_KEYS.PRICE_HISTORY)
    const totalPages = Math.ceil(total / perPage)

    // Ambil dari akhir (terbaru) dengan pagination
    const start = Math.max(0, total - (page * perPage))
    const end = total - ((page - 1) * perPage) - 1

    const items = await redis.lrange(REDIS_KEYS.PRICE_HISTORY, start, end)

    return {
      items: items.reverse(),
      page: page,
      perPage: perPage,
      total: total,
      totalPages: totalPages
    }
  } catch (e) {
    return { items: [], page: 1, perPage: 10, total: 0, totalPages: 0 }
  }
}

// Reset data harian setiap jam 23:59 WIB
async function resetDailyData() {
  try {
    await Promise.all([
      redis.del(REDIS_KEYS.DAILY_STATS),
      redis.del(REDIS_KEYS.PRICE_HISTORY)
    ])
    dailyStatsCache = null
    priceHistoryCache = []
    pushLog('SYSTEM | Daily reset completed')
  } catch (e) {
    pushLog('REDIS | Reset error: ' + e.message)
  }
}

// Cek setiap menit untuk reset jam 23:59
setInterval(() => {
  const now = new Date()
  // Konversi ke WIB (UTC+7)
  const wibHour = (now.getUTCHours() + 7) % 24
  const wibMinute = now.getUTCMinutes()

  // Reset pada 23:59 WIB
  if (wibHour === 23 && wibMinute === 59) {
    resetDailyData()
  }
}, 60000)

// Lock untuk mencegah double fetch USD/IDR
let isUsdIdrFetching = false

// Background task untuk pre-fetch market data
// USD/IDR fetched setiap menit (sama seperti ketik "emas")
// XAU/USD and calendar updated every 5 seconds
setInterval(async () => {
  try {
    const now = Date.now()
    const currentMinute = Math.floor(now / 60000)
    const lastFetchMinute = Math.floor(cachedMarketData.lastUsdIdrFetch / 60000)

    // Fetch USD/IDR setiap ganti menit (dengan lock untuk mencegah double fetch)
    let usdIdr = cachedMarketData.usdIdr;
    if ((currentMinute !== lastFetchMinute || cachedMarketData.lastUsdIdrFetch === 0) && !isUsdIdrFetching) {
      isUsdIdrFetching = true
      try {
        usdIdr = await fetchUSDIDRFromGoogle();
        cachedMarketData.lastUsdIdrFetch = now
      } catch (e) {
        // Keep old USD/IDR if fetch fails
      } finally {
        isUsdIdrFetching = false
      }
    }

    // Always fetch XAU/USD and economic calendar
    const [xauUsd, economicEvents] = await Promise.all([
      fetchXAUUSDCached(),
      fetchEconomicCalendar()
    ]);

    cachedMarketData = {
      ...cachedMarketData,
      usdIdr,
      xauUsd,
      economicEvents,
      lastUpdate: now
    }
  } catch (e) {
    // Silent fail - keep old cache
  }
}, 5000) // Check every 5 seconds, USD/IDR setiap ganti menit

function pushLog(s) {
  const now = new Date()
  const time = now.toTimeString().substring(0, 8)
  const logMsg = `[${time}] ${s}`
  logs.push(logMsg)
  if (logs.length > 30) logs.shift()
  console.log(logMsg)
}

setInterval(() => {
  if (processedMsgIds.size > 300) {
    const arr = Array.from(processedMsgIds).slice(-200)
    processedMsgIds.clear()
    arr.forEach(id => processedMsgIds.add(id))
  }
}, 5 * 60 * 1000)

// ------ UTIL ------
function normalizeText(msg) {
  if (!msg) return ''
  return msg.replace(/\s+/g, ' ').trim().toLowerCase()
}

function shouldIgnoreMessage(m) {
  if (!m || !m.key) return true
  if (m.key.remoteJid === 'status@broadcast') return true
  if (m.key.fromMe) return true
  
  const hasText =
    m.message?.conversation ||
    m.message?.extendedTextMessage?.text ||
    m.message?.imageMessage?.caption ||
    m.message?.videoMessage?.caption
  if (!hasText) return true
  
  return false
}

function extractText(m) {
  return (
    m.message?.conversation ||
    m.message?.extendedTextMessage?.text ||
    m.message?.imageMessage?.caption ||
    m.message?.videoMessage?.caption ||
    ''
  )
}

function formatRupiah(n) {
  return typeof n === 'number'
    ? n.toLocaleString('id-ID')
    : (Number(n || 0) || 0).toLocaleString('id-ID')
}

function calculateDiscount(investmentAmount) {
  const MAX_DISCOUNT = 1020000
  
  let discountPercent
  
  if (investmentAmount <= 250000) {
    discountPercent = 3.0
  } else if (investmentAmount <= 5000000) {
    discountPercent = 3.4
  } else if (investmentAmount <= 10000000) {
    discountPercent = 3.45
  } else if (investmentAmount <= 20000000) {
    discountPercent = 3.425
  } else {
    discountPercent = 3.4
  }
  
  const calculatedDiscount = investmentAmount * (discountPercent / 100)
  return Math.min(calculatedDiscount, MAX_DISCOUNT)
}

function calculateProfit(buyRate, sellRate, investmentAmount) {
  const discountAmount = calculateDiscount(investmentAmount)
  const discountedPrice = investmentAmount - discountAmount
  const totalGrams = investmentAmount / buyRate
  const sellValue = totalGrams * sellRate
  const totalProfit = sellValue - discountedPrice
  
  return {
    discountedPrice,
    totalGrams,
    profit: totalProfit
  }
}

// ------ ECONOMIC CALENDAR FUNCTIONS ------
async function fetchEconomicCalendar() {
  if (!ECONOMIC_CALENDAR_ENABLED) return null
  
  const now = Date.now()
  
  if (cachedEconomicEvents && (now - lastEconomicFetch) < ECONOMIC_CACHE_DURATION) {
    return cachedEconomicEvents
  }
  
  try {
    const res = await fetch('https://nfs.faireconomy.media/ff_calendar_thisweek.json', {
      signal: AbortSignal.timeout(5000)
    })
    
    if (!res.ok) {
      // Silent fail
      return null
    }
    
    const events = await res.json()
    
    // Waktu Jakarta (WIB = UTC+7)
    const jakartaNow = new Date(Date.now() + (7 * 60 * 60 * 1000))
    const todayJakarta = new Date(jakartaNow.getFullYear(), jakartaNow.getMonth(), jakartaNow.getDate())
    const tomorrowJakarta = new Date(todayJakarta.getTime() + (24 * 60 * 60 * 1000))
    const dayAfterTomorrowJakarta = new Date(todayJakarta.getTime() + (2 * 24 * 60 * 60 * 1000))
    
    const filteredEvents = events.filter(event => {
      if (!event.date) return false
      
      // Parse event date dan convert ke WIB
      const eventDate = new Date(event.date)
      const eventWIB = new Date(eventDate.getTime() + (7 * 60 * 60 * 1000))
      const eventDateOnly = new Date(eventWIB.getFullYear(), eventWIB.getMonth(), eventWIB.getDate())
      
      // ⏰ LOGIC: Tampilkan news 3 jam setelah rilis
      const threeHoursAfterEvent = new Date(eventDate.getTime() + (3 * 60 * 60 * 1000))
      
      // Jika news sudah lewat 3 jam, skip
      if (Date.now() > threeHoursAfterEvent.getTime()) {
        return false
      }
      
      // Filter: hanya hari ini dan besok (2 hari)
      if (eventDateOnly < todayJakarta || eventDateOnly >= dayAfterTomorrowJakarta) {
        return false
      }
      
      // Filter: hanya USD
      if (!CALENDAR_COUNTRY_FILTER.includes(event.country)) return false
      
      // Filter: hanya High Impact
      if (event.impact !== 'High') return false
      
      return true
    })
    
    // Sort by time
    filteredEvents.sort((a, b) => {
      const timeA = new Date(a.date).getTime()
      const timeB = new Date(b.date).getTime()
      return timeA - timeB
    })
    
    // Limit to 10 events
    const limitedEvents = filteredEvents.slice(0, 10)
    
    // Calendar loaded silently
    
    cachedEconomicEvents = limitedEvents
    lastEconomicFetch = now
    
    return limitedEvents
    
  } catch (e) {
    // Silent fail
    return null
  }
}

// Fungsi untuk menentukan apakah news bagus/jelek untuk gold
function analyzeGoldImpact(event) {
  const title = (event.title || '').toLowerCase()
  const actual = event.actual || ''
  const forecast = event.forecast || ''
  
  if (!actual || actual === '-' || !forecast || forecast === '-') {
    return null
  }
  
  const actualNum = parseFloat(actual.replace(/[^0-9.-]/g, ''))
  const forecastNum = parseFloat(forecast.replace(/[^0-9.-]/g, ''))
  
  if (isNaN(actualNum) || isNaN(forecastNum)) {
    return null
  }
  
  // Logic: news yang memperkuat USD = jelek untuk gold
  // news yang melemahkan USD = bagus untuk gold
  
  // Interest Rate: Naik = USD kuat = jelek untuk gold
  if (title.includes('interest rate') || title.includes('fed') || title.includes('fomc')) {
    return actualNum > forecastNum ? 'JELEK' : 'BAGUS'
  }
  
  // NFP / Employment: Naik = ekonomi kuat = USD kuat = jelek untuk gold
  if (title.includes('non-farm') || title.includes('nfp') || title.includes('payroll')) {
    return actualNum > forecastNum ? 'JELEK' : 'BAGUS'
  }
  
  // Unemployment: Naik = ekonomi lemah = USD lemah = bagus untuk gold
  if (title.includes('unemployment')) {
    return actualNum > forecastNum ? 'BAGUS' : 'JELEK'
  }
  
  // CPI / Inflation: Naik = inflasi tinggi = bagus untuk gold
  if (title.includes('cpi') || title.includes('inflation') || title.includes('pce')) {
    return actualNum > forecastNum ? 'BAGUS' : 'JELEK'
  }
  
  // GDP: Naik = ekonomi kuat = USD kuat = jelek untuk gold
  if (title.includes('gdp')) {
    return actualNum > forecastNum ? 'JELEK' : 'BAGUS'
  }
  
  // Jobless Claims: Naik = ekonomi lemah = bagus untuk gold
  if (title.includes('jobless') || title.includes('claims')) {
    return actualNum > forecastNum ? 'BAGUS' : 'JELEK'
  }
  
  // Retail Sales: Naik = ekonomi kuat = jelek untuk gold
  if (title.includes('retail sales')) {
    return actualNum > forecastNum ? 'JELEK' : 'BAGUS'
  }
  
  return null
}

function formatEconomicCalendar(events) {
  if (!events || events.length === 0) {
    return ''
  }
  
  let calendarText = '\n📅 USD News\n'
  
  events.forEach((event, index) => {
    const eventDate = new Date(event.date)
    const wibTime = new Date(eventDate.getTime() + (7 * 60 * 60 * 1000))
    
    const minutes = wibTime.getMinutes()
    const roundedMinutes = Math.round(minutes / 5) * 5
    wibTime.setMinutes(roundedMinutes)
    wibTime.setSeconds(0)
    
    const hours = wibTime.getHours().toString().padStart(2, '0')
    const mins = wibTime.getMinutes().toString().padStart(2, '0')
    const timeStr = `${hours}:${mins}`
    
    const days = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu']
    const dayName = days[wibTime.getDay()]
    
    const title = event.title || event.event || 'Unknown Event'
    const forecast = event.forecast || '-'
    const previous = event.previous || '-'
    const actual = event.actual || '-'
    
    const nowTime = Date.now()
    const eventTime = eventDate.getTime()
    const timeSinceEvent = nowTime - eventTime
    const minutesSinceEvent = Math.floor(timeSinceEvent / (60 * 1000))
    
    let timeStatus = ''
    if (timeSinceEvent < 0) {
      const minutesUntil = Math.abs(minutesSinceEvent)
      if (minutesUntil < 60) {
        timeStatus = `⏰${minutesUntil}m`
      } else {
        const hoursUntil = Math.floor(minutesUntil / 60)
        const minsUntil = minutesUntil % 60
        if (minsUntil > 0) {
          timeStatus = `⏰${hoursUntil}j ${minsUntil}m`
        } else {
          timeStatus = `⏰${hoursUntil}j`
        }
      }
    } else if (timeSinceEvent > 0 && timeSinceEvent <= 3 * 60 * 60 * 1000) {
      const hoursAgo = Math.floor(minutesSinceEvent / 60)
      const minsAgo = minutesSinceEvent % 60
      if (hoursAgo > 0) {
        timeStatus = `✅${hoursAgo}j ${minsAgo}m lalu`
      } else {
        timeStatus = `✅${minsAgo}m lalu`
      }
    }
    
    // Shortened title
    let shortTitle = title
    if (title.includes('Non-Farm')) shortTitle = 'NFP'
    else if (title.includes('Unemployment')) shortTitle = 'Unemp'
    else if (title.includes('Interest Rate')) shortTitle = 'Interest'
    else if (title.includes('CPI')) shortTitle = 'CPI'
    else if (title.includes('GDP')) shortTitle = 'GDP'
    else if (title.includes('Retail')) shortTitle = 'Retail'
    else if (title.includes('Jobless')) shortTitle = 'Jobless'
    
    calendarText += `• ${dayName} ${timeStr}`
    
    if (timeStatus) {
      calendarText += ` (${timeStatus})`
    }
    
    calendarText += ` ${shortTitle}`
    
    if (actual !== '-' && actual !== '') {
      const goldImpact = analyzeGoldImpact(event)
      
      calendarText += ` ${actual}>${forecast}`
      
      if (goldImpact === 'BAGUS') {
        calendarText += ` 🟢 BAGUS`
      } else if (goldImpact === 'JELEK') {
        calendarText += ` 🔴 JELEK`
      }
    } else if (forecast !== '-') {
      calendarText += ` F:${forecast}`
    }
    
    calendarText += '\n'
  })
  
  return calendarText
}

// ------ FOREX FUNCTIONS ------
async function fetchUSDIDRFromBankIndonesia() {
  try {
    // Try to fetch from Bank Indonesia JISDOR
    const res = await fetch('https://api.exchangerate-api.com/v4/latest/USD', {
      signal: AbortSignal.timeout(2000)
    })
    if (res.ok) {
      const json = await res.json()
      const rate = json.rates?.IDR
      if (rate && rate > 10000 && rate < 20000) {
        return { rate }
      }
    }
  } catch (_) {}
  return null
}

async function fetchUSDIDRFallback() {
  try {
    // Try multiple sources for better accuracy
    const sources = [
      // Primary: ExchangeRate-API
      async () => {
        const res = await fetch('https://api.exchangerate-api.com/v4/latest/USD', {
          signal: AbortSignal.timeout(2000)
        })
        if (res.ok) {
          const json = await res.json()
          return json.rates?.IDR
        }
      },
      // Secondary: Fixer.io (free tier)
      async () => {
        const res = await fetch('https://api.fixer.io/latest?base=USD&symbols=IDR', {
          signal: AbortSignal.timeout(2000)
        })
        if (res.ok) {
          const json = await res.json()
          return json.rates?.IDR
        }
      }
    ]

    for (const source of sources) {
      try {
        const rate = await source()
        if (rate && rate > 10000 && rate < 20000) {
          return { rate }
        }
      } catch (_) {}
    }
  } catch (_) {}

  return { rate: 16600 }
}

async function fetchUSDIDRFromGoogle() {
  const maxRetries = 3
  let attempt = 0

  while (attempt < maxRetries) {
    attempt++

    try {
      const res = await fetch('https://www.google.com/finance/quote/USD-IDR', {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
          'Accept-Encoding': 'gzip, deflate, br',
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache',
          'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
          'Sec-Ch-Ua-Mobile': '?0',
          'Sec-Ch-Ua-Platform': '"Windows"',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'none',
          'Sec-Fetch-User': '?1',
          'Upgrade-Insecure-Requests': '1'
        },
        signal: AbortSignal.timeout(10000) // Increased timeout to 10 seconds
      })

      if (!res.ok) {
        if (attempt < maxRetries) {
          await new Promise(r => setTimeout(r, 2000))
          continue
        }
      }

      const html = await res.text()

      // More comprehensive patterns for Google Finance
      const patterns = [
        // Primary patterns - most likely to work
        /class="YMlKec fxKbKc"[^>]*>([0-9,\.]+)<\/div>/i,
        /class="[^"]*fxKbKc[^"]*"[^>]*>([0-9,\.]+)<\/div>/i,
        /data-last-price="([0-9,\.]+)"/i,
        /data-price="([0-9,\.]+)"/i,

        // JSON-LD patterns
        /"price":\s*"([0-9,\.]+)"/i,
        /"value":\s*"([0-9,\.]+)"/i,

        // Alternative div patterns
        /<div[^>]*>([0-9]{1,2}[,\.][0-9]{3}(?:\.[0-9]+)?)<\/div>/i,

        // Specific Google Finance patterns
        /USD to IDR[^0-9]*([0-9]{1,2}[,\.][0-9]{3}(?:\.[0-9]+)?)/i,
        /1 USD = ([0-9]{1,2}[,\.][0-9]{3}(?:\.[0-9]+)?)/i,

        // Meta tag patterns
        /<meta[^>]*content="([0-9]{1,2}[,\.][0-9]{3}(?:\.[0-9]+)?)"[^>]*>/i,

        // Broader patterns
        />([0-9]{2}[,\.][0-9]{3}(?:\.[0-9]+)?)</,
        /USD\/IDR[^0-9]*([0-9]{1,2}[,\.][0-9]{3}(?:[,\.][0-9]+)?)/i
      ]

      // Silent parsing - no log needed

      for (const pattern of patterns) {
        const match = html.match(pattern)
        if (match?.[1]) {
          const rate = parseFloat(match[1].replace(/,/g, ''))

          // Validate rate is in reasonable range for IDR
          if (rate > 10000 && rate < 20000) {
            return { rate }
          }
        }
      }

      // Silent retry
      if (attempt < maxRetries) {
        await new Promise(r => setTimeout(r, 3000))
      }

    } catch (err) {
      if (attempt < maxRetries) {
        await new Promise(r => setTimeout(r, 3000))
      }
    }
  }

  return { rate: 15900 }
}

async function fetchXAUUSDFromTradingView() {
  try {
    const res = await fetch('https://scanner.tradingview.com/symbol', {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      body: JSON.stringify({
        symbols: {
          tickers: ['OANDA:XAUUSD'],
          query: { types: [] }
        },
        columns: ['close']
      }),
      signal: AbortSignal.timeout(5000)
    })
    
    if (res.ok) {
      const json = await res.json()
      if (json?.data?.[0]?.d) {
        const price = json.data[0].d[0]

        if (price > 1000 && price < 10000) {
          // Silent success - no log needed
          return price
        }
      }
    }
  } catch (e) {
    // Silent fail - will try next source
  }
  return null
}

async function fetchXAUUSDFromInvesting() {
  try {
    const res = await fetch('https://www.investing.com/currencies/xau-usd', {
      headers: { 
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
      },
      signal: AbortSignal.timeout(6000)
    })
    
    if (!res.ok) {
      // Silent fail
      return null
    }
    
    const html = await res.text()
    const foundPrices = []
    
    let match = html.match(/data-test="instrument-price-last"[^>]*>([0-9,]+\.?[0-9]*)</i)
    if (match?.[1]) {
      const price = parseFloat(match[1].replace(/,/g, ''))
      if (price > 1000 && price < 10000) {
        foundPrices.push({ method: 'data-test', price, priority: 1 })
      }
    }

    match = html.match(/class="instrument-price-last[^"]*"[^>]*>([0-9,]+\.?[0-9]*)</i)
    if (match?.[1]) {
      const price = parseFloat(match[1].replace(/,/g, ''))
      if (price > 1000 && price < 10000) {
        foundPrices.push({ method: 'class-instrument', price, priority: 2 })
      }
    }

    const pricePatterns = [
      /instrument[^>]{0,50}([0-9]{1},?[0-9]{3}\.[0-9]{2})/i,
      /quote[^>]{0,50}([0-9]{1},?[0-9]{3}\.[0-9]{2})/i,
      /current[^>]{0,50}([0-9]{1},?[0-9]{3}\.[0-9]{2})/i
    ]

    for (const pattern of pricePatterns) {
      match = html.match(pattern)
      if (match?.[1]) {
        const price = parseFloat(match[1].replace(/,/g, ''))
        if (price > 1000 && price < 10000) {
          foundPrices.push({ method: 'generic-pattern', price, priority: 9 })
        }
      }
    }

    if (foundPrices.length === 0) {
      return null
    }

    if (foundPrices.length === 1) {
      return foundPrices[0].price
    }

    const priceGroups = new Map()

    for (const { method, price, priority } of foundPrices) {
      let foundGroup = false

      for (const [groupPrice, items] of priceGroups) {
        if (Math.abs(groupPrice - price) <= 1.0) {
          items.push({ method, price, priority })
          foundGroup = true
          break
        }
      }

      if (!foundGroup) {
        priceGroups.set(price, [{ method, price, priority }])
      }
    }

    let bestGroup = null
    let maxCount = 0
    let bestPriority = 999

    for (const [groupPrice, items] of priceGroups) {
      const avgPriority = items.reduce((sum, item) => sum + item.priority, 0) / items.length

      if (items.length > maxCount) {
        maxCount = items.length
        bestGroup = items
        bestPriority = avgPriority
      } else if (items.length === maxCount && avgPriority < bestPriority) {
        bestGroup = items
        bestPriority = avgPriority
      }
    }

    if (bestGroup) {
      const avgPrice = bestGroup.reduce((sum, item) => sum + item.price, 0) / bestGroup.length
      return avgPrice
    }

    foundPrices.sort((a, b) => a.priority - b.priority)
    const fallbackPrice = foundPrices[0].price

    return fallbackPrice
    
  } catch (e) {
    // Silent fail
    return null
  }
}

async function fetchXAUUSDFromGoogle() {
  try {
    const res = await fetch('https://www.google.com/finance/quote/XAU-USD', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
      signal: AbortSignal.timeout(3000)
    })
    
    if (res.ok) {
      const html = await res.text()
      let priceMatch = html.match(/class="YMlKec fxKbKc"[^>]*>([0-9,\.]+)<\/div>/i)
      if (!priceMatch) priceMatch = html.match(/class="[^"]*fxKbKc[^"]*"[^>]*>([0-9,\.]+)<\/div>/i)
      
      if (priceMatch?.[1]) {
        const price = parseFloat(priceMatch[1].replace(/,/g, ''))
        if (price > 1000 && price < 10000) {
          // Silent success
          return price
        }
      }
    }
  } catch (e) {
    // Silent fail
  }
  return null
}

async function fetchXAUUSD() {
  let result = await fetchXAUUSDFromTradingView()
  if (result) return result

  result = await fetchXAUUSDFromInvesting()
  if (result) return result

  result = await fetchXAUUSDFromGoogle()
  if (result) return result

  return null
}

async function fetchXAUUSDCached() {
  const now = Date.now()

  if (cachedXAUUSD && (now - lastXAUUSDFetch) < XAU_CACHE_DURATION) {
    return cachedXAUUSD
  }

  const price = await fetchXAUUSD()
  if (price) {
    cachedXAUUSD = price
    lastXAUUSDFetch = now

    // Simpan ke history untuk chart
    xauHistory.push({
      time: now,
      price: price
    })

    // Batasi jumlah history
    if (xauHistory.length > MAX_XAU_HISTORY) {
      xauHistory.shift()
    }
  }

  return cachedXAUUSD
}

function analyzePriceStatus(treasuryBuy, treasurySell, xauUsdPrice, usdIdrRate) {
  if (!xauUsdPrice || !usdIdrRate) {
    return {
      status: 'DATA_INCOMPLETE',
      message: '⚠️ Data Incomplete',
      emoji: '⚠️'
    }
  }

  // Range NORMAL: margin 0.97% - 1.25%
  const TROY_OZ_TO_GRAM_EXACT = 31.1035
  const MIN_MARGIN = 1.0097  // 0.97%
  const MAX_MARGIN = 1.0125  // 1.25%

  // Hitung harga dasar internasional
  const basePrice = (xauUsdPrice * usdIdrRate) / TROY_OZ_TO_GRAM_EXACT

  // Hitung batas bawah dan atas untuk range NORMAL
  const lowerBound = basePrice * MIN_MARGIN
  const upperBound = basePrice * MAX_MARGIN

  // Hitung selisih dari range NORMAL
  let difference = 0
  let status = 'NORMAL'
  let emoji = '✅'
  let message = '✅ NORMAL'

  if (treasurySell < lowerBound) {
    // Di bawah range NORMAL (margin < 1.2%)
    difference = treasurySell - lowerBound  // akan negatif
    status = 'ABNORMAL'
    emoji = '⚠️'
    message = `⚠️ TIDAK NORMAL (${difference > 0 ? '+' : ''}${formatRupiah(Math.round(difference))})`
  } else if (treasurySell > upperBound) {
    // Di atas range NORMAL (margin > 1.35%)
    difference = treasurySell - upperBound  // akan positif
    status = 'ABNORMAL'
    emoji = '⚠️'
    message = `⚠️ TIDAK NORMAL (+${formatRupiah(Math.round(difference))})`
  }

  // Calculate actual margin percentage
  const actualMargin = ((treasurySell - basePrice) / basePrice) * 100

  // Log only once per minute or when status changes (removed repetitive logging)

  return {
    status,
    emoji,
    message,
    basePrice,
    lowerBound,
    upperBound,
    treasuryPrice: treasurySell,
    difference,
    actualMargin
  }
}

function formatMessage(treasuryData, usdIdrRate, xauUsdPrice = null, priceChange = null, economicEvents = null) {
  const buy = treasuryData?.data?.buying_rate || 0
  const sell = treasuryData?.data?.selling_rate || 0

  const spread = sell - buy
  const spreadPercent = ((spread / buy) * 100).toFixed(2)

  const buyFormatted = `Rp${formatRupiah(buy)}/gr`
  const sellFormatted = `Rp${formatRupiah(sell)}/gr`

  const updatedAt = treasuryData?.data?.updated_at
  let timeSection = ''
  if (updatedAt) {
    const date = new Date(updatedAt)
    const days = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu']
    const dayName = days[date.getDay()]
    const hours = date.getHours().toString().padStart(2, '0')
    const minutes = date.getMinutes().toString().padStart(2, '0')
    const seconds = date.getSeconds().toString().padStart(2, '0')
    timeSection = `${dayName} ${hours}:${minutes}:${seconds} WIB`
  }

  let headerSection = ''
  if (priceChange && priceChange.buyChange !== 0) {
    const changeAmount = Math.abs(priceChange.buyChange)
    const changeFormatted = formatRupiah(changeAmount)
    if (priceChange.buyChange > 0) {
      headerSection = `🚀 🚀 NAIK 🚀 🚀 (+Rp${changeFormatted})\n`
    } else {
      headerSection = `🔻 🔻 TURUN 🔻 🔻 (-Rp${changeFormatted})\n`
    }
  }

  // Analisis status harga dengan rumus user
  let statusSection = ''
  if (xauUsdPrice && usdIdrRate) {
    const priceStatus = analyzePriceStatus(buy, sell, xauUsdPrice, usdIdrRate)
    statusSection = `\n${priceStatus.message}`
  }

  let marketSection = `💱 USD Rp${formatRupiah(Math.round(usdIdrRate))}`

  if (xauUsdPrice) {
    marketSection += ` | XAU $${xauUsdPrice.toFixed(2)}`
  }

  const calendarSection = formatEconomicCalendar(economicEvents)

  const grams20M = calculateProfit(buy, sell, 20000000).totalGrams
  const profit20M = calculateProfit(buy, sell, 20000000).profit
  const grams30M = calculateProfit(buy, sell, 30000000).totalGrams
  const profit30M = calculateProfit(buy, sell, 30000000).profit

  // Format gram dengan 4 digit desimal
  const formatGrams = (g) => g.toFixed(4)

  return `${headerSection}${timeSection}${statusSection}

💰 Beli ${buyFormatted} | Jual ${sellFormatted} (${spreadPercent > 0 ? '-' : ''}${spreadPercent}%)
${marketSection}

🎁 20jt→${formatGrams(grams20M)}gr (+Rp${formatRupiah(Math.round(profit20M))}) | 30jt→${formatGrams(grams30M)}gr (+Rp${formatRupiah(Math.round(profit30M))})
${calendarSection}
⚡ Auto-update`
}
async function fetchTreasury() {
  const res = await fetch(TREASURY_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Connection': 'keep-alive'
    },
    agent: httpsAgent, // Reuse TCP connection
    signal: AbortSignal.timeout(1500) // 1.5 detik timeout (lebih agresif)
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  const json = await res.json()
  if (!json?.data?.buying_rate || !json?.data?.selling_rate) {
    throw new Error('Invalid data')
  }
  return json
}

// ⚡ ULTRA-INSTANT BROADCAST - Message sudah di-build sebelumnya
function doBroadcastInstant(message) {
  // Simpan pesan untuk monitoring (selalu update meski tidak ada subscriber)
  lastBroadcastMessage = message

  if (!sock || !isReady || subscriptions.size === 0) return

  broadcastCount++
  const currentBroadcastId = broadcastCount
  const subsCount = subscriptions.size

  // 🚀 INSTANT: Fire semua sekaligus tanpa await
  const chatIds = Array.from(subscriptions)
  for (let i = 0; i < chatIds.length; i++) {
    sock.sendMessage(chatIds[i], { text: message }).catch(() => {})
  }

  pushLog(`SEND | Broadcast #${currentBroadcastId} to ${subsCount} subscribers`)
}

let isPriceChecking = false // Lock untuk mencegah overlap

// ==================== MULTI-INTERVAL SPEED TEST ====================
const INTERVALS = [100, 200, 300, 500] // Interval yang ditest (ms)
let currentIntervalIndex = 0
let intervalStats = {}
let lastPriceChangeTime = null
let lastApiUpdateTime = null

// Initialize stats untuk setiap interval
INTERVALS.forEach(interval => {
  intervalStats[interval] = {
    attempts: 0,
    successes: 0,
    totalDelay: 0,
    minDelay: Infinity,
    maxDelay: 0,
    avgDelay: 0,
    errors: 0
  }
})

async function checkPriceUpdate() {
  if (isPriceChecking) return // Skip jika masih fetching
  isPriceChecking = true

  const currentInterval = INTERVALS[currentIntervalIndex]

  // Selalu fetch price untuk monitoring web, broadcast hanya jika ada subscriber
  try {
    const fetchStart = Date.now()
    const treasuryData = await fetchTreasury()
    const fetchTime = Date.now() - fetchStart
    const currentPrice = {
      buy: treasuryData?.data?.buying_rate,
      sell: treasuryData?.data?.selling_rate,
      updated_at: treasuryData?.data?.updated_at,
      fetchedAt: Date.now()
    }

    intervalStats[currentInterval].attempts++

    // Cek apakah API time berubah (harga baru dari Treasury)
    const apiTime = currentPrice.updated_at
    if (apiTime && apiTime !== lastApiUpdateTime) {
      const delayMs = Date.now() - new Date(apiTime).getTime()

      // Update stats
      intervalStats[currentInterval].successes++
      intervalStats[currentInterval].totalDelay += delayMs
      if (delayMs < intervalStats[currentInterval].minDelay) {
        intervalStats[currentInterval].minDelay = delayMs
      }
      if (delayMs > intervalStats[currentInterval].maxDelay) {
        intervalStats[currentInterval].maxDelay = delayMs
      }

      lastApiUpdateTime = apiTime
    }

    // Rotate interval untuk test berikutnya
    currentIntervalIndex = (currentIntervalIndex + 1) % INTERVALS.length

    if (!lastKnownPrice) {
      lastKnownPrice = currentPrice
      lastBroadcastedPrice = currentPrice
      lastPriceUpdateTime = Date.now()
      await updateDailyStats(currentPrice.buy)
      pushLog(`PRICE | Initial: Buy ${formatRupiah(currentPrice.buy)} | Sell ${formatRupiah(currentPrice.sell)}`)

      // Check initial price status
      if (cachedMarketData.xauUsd && cachedMarketData.usdIdr) {
        const priceStatus = analyzePriceStatus(
          currentPrice.buy,
          currentPrice.sell,
          cachedMarketData.xauUsd,
          cachedMarketData.usdIdr.rate
        )
        if (priceStatus.status === 'ABNORMAL') {
          pushLog(`PRICE | Initial status: ABNORMAL`)
        }
      }
      return
    }
    
    const buyChanged = lastKnownPrice.buy !== currentPrice.buy
    const sellChanged = lastKnownPrice.sell !== currentPrice.sell

    // ⏱️ STALE PRICE DETECTION
    const now = Date.now()
    const timeSinceLastUpdate = now - lastPriceUpdateTime
    const isPriceStale = timeSinceLastUpdate >= STALE_PRICE_THRESHOLD

    // Check jika status berubah dari NORMAL ke TIDAK NORMAL atau sebaliknya
    let statusChanged = false
    let currentStatus = null
    let previousStatus = null

    if (cachedMarketData.xauUsd && cachedMarketData.usdIdr) {
      const currentPriceStatus = analyzePriceStatus(
        currentPrice.buy,
        currentPrice.sell,
        cachedMarketData.xauUsd,
        cachedMarketData.usdIdr.rate
      )
      currentStatus = currentPriceStatus.status

      const lastPriceStatus = analyzePriceStatus(
        lastKnownPrice.buy,
        lastKnownPrice.sell,
        cachedMarketData.xauUsd,
        cachedMarketData.usdIdr.rate
      )
      previousStatus = lastPriceStatus.status

      statusChanged = currentStatus !== previousStatus

      if (statusChanged) {
        if (currentStatus === 'ABNORMAL') {
          pushLog(`PRICE | Status changed: NORMAL -> ABNORMAL`)
        } else if (currentStatus === 'NORMAL') {
          pushLog(`PRICE | Status changed: ABNORMAL -> NORMAL`)
        }
      }
    }

    // Cek apakah data lebih baru berdasarkan updated_at
    const currentUpdatedAt = new Date(currentPrice.updated_at).getTime()
    const lastUpdatedAt = lastKnownPrice.updated_at ? new Date(lastKnownPrice.updated_at).getTime() : 0

    // SKIP jika data dari API lebih lama dari yang sudah ada
    if (currentUpdatedAt < lastUpdatedAt) {
      pushLog(`PRICE | Skip old data: ${currentPrice.updated_at} < ${lastKnownPrice.updated_at}`)
      return
    }

    // Selalu update lastKnownPrice untuk monitoring web
    const prevPrice = { ...lastKnownPrice }
    lastKnownPrice = currentPrice

    // Update daily stats only (history handled by fastPoll)
    if (buyChanged) {
      await updateDailyStats(currentPrice.buy)
    }

    // INSTANT SSE PUSH ke frontend monitoring
    if (buyChanged || sellChanged) {
      const sseData = {
        type: 'price',
        buy: currentPrice.buy,
        sell: currentPrice.sell,
        prevBuy: prevPrice.buy,
        prevSell: prevPrice.sell,
        updatedAt: currentPrice.updated_at,
        usdIdr: cachedMarketData.usdIdr?.rate,
        xauUsd: cachedMarketData.xauUsd,
        serverTime: new Date().toISOString()
      }
      broadcastSSE(sseData)
    }

    if (!buyChanged && !sellChanged) {
      return
    }

    // Skip WA broadcast jika tidak ada subscriber
    if (!isReady || subscriptions.size === 0) {
      return
    }
    
    // 🔥 ADA PERUBAHAN HARGA!
    const buyChangeSinceBroadcast = Math.abs(currentPrice.buy - (lastBroadcastedPrice?.buy || currentPrice.buy))
    const sellChangeSinceBroadcast = Math.abs(currentPrice.sell - (lastBroadcastedPrice?.sell || currentPrice.sell))
    
    if (buyChangeSinceBroadcast < MIN_PRICE_CHANGE && sellChangeSinceBroadcast < MIN_PRICE_CHANGE) {
      lastPriceUpdateTime = now  // Update timestamp meskipun perubahan kecil
      return
    }
    
    const timeSinceLastBroadcast = now - lastBroadcastTime
    
    // Cek apakah sudah ganti menit
    const lastBroadcastDate = new Date(lastBroadcastTime)
    const currentDate = new Date(now)
    const lastMinute = lastBroadcastDate.getHours() * 60 + lastBroadcastDate.getMinutes()
    const currentMinute = currentDate.getHours() * 60 + currentDate.getMinutes()
    const isNewMinute = currentMinute !== lastMinute
    
    // 🚫 CEK DULU: Apakah sudah broadcast di menit ini?
    const alreadyBroadcastThisMinute = lastBroadcastMinute === currentMinute

    // 🎯 LOGIKA BROADCAST:
    // 1. Jika sudah broadcast di menit ini → SKIP (hindari 2x broadcast per menit)
    // 2. Jika status berubah ke TIDAK NORMAL → BROADCAST LANGSUNG (prioritas tinggi!)
    // 3. Jika harga stale (5+ menit tidak update) → BROADCAST LANGSUNG saat ada update baru
    // 4. Jika harga tidak stale → ikuti cooldown normal (50 detik ATAU ganti menit)

    const shouldBroadcast = alreadyBroadcastThisMinute
      ? false  // 🚫 Sudah broadcast di menit ini, skip!
      : statusChanged && currentStatus === 'ABNORMAL'
      ? true  // Langsung broadcast jika status berubah ke TIDAK NORMAL
      : isPriceStale
      ? true  // Langsung broadcast jika harga baru setelah 5 menit stale
      : (timeSinceLastBroadcast >= BROADCAST_COOLDOWN || isNewMinute)
    
    if (!shouldBroadcast) {
      const priceChange = {
        buyChange: currentPrice.buy - prevPrice.buy,
        sellChange: currentPrice.sell - prevPrice.sell
      }

      lastPriceUpdateTime = now  // Update timestamp

      const time = new Date().toISOString().substring(11, 19)
      const buyIcon = priceChange.buyChange > 0 ? '📈' : '📉'
      const sellIcon = priceChange.sellChange > 0 ? '📈' : '📉'

      // Log dengan reason yang tepat
      const skipReason = alreadyBroadcastThisMinute
        ? 'sudah kirim menit ini'
        : `tunggu ${Math.round((BROADCAST_COOLDOWN - timeSinceLastBroadcast)/1000)}s`

      pushLog(`PRICE | ${buyIcon}Buy ${priceChange.buyChange > 0 ? '+' : ''}${formatRupiah(priceChange.buyChange)} ${sellIcon}Sell ${priceChange.sellChange > 0 ? '+' : ''}${formatRupiah(priceChange.sellChange)} → skip (${skipReason})`)
      return
    }

    const priceChange = {
      buyChange: currentPrice.buy - prevPrice.buy,
      sellChange: currentPrice.sell - prevPrice.sell
    }

    lastPriceUpdateTime = now  // Update timestamp saat broadcast
    
    const buyIcon = priceChange.buyChange > 0 ? '📈' : '📉'
    const sellIcon = priceChange.sellChange > 0 ? '📈' : '📉'

    pushLog(`PRICE | ${buyIcon}Buy ${priceChange.buyChange > 0 ? '+' : ''}${formatRupiah(priceChange.buyChange)} ${sellIcon}Sell ${priceChange.sellChange > 0 ? '+' : ''}${formatRupiah(priceChange.sellChange)} → BROADCAST`)
    
    // CRITICAL FIX: Hitung finalPriceChange SEBELUM update lastBroadcastedPrice
    const finalPriceChange = {
      buyChange: currentPrice.buy - lastBroadcastedPrice.buy,
      sellChange: currentPrice.sell - lastBroadcastedPrice.sell
    }
    
    // ✅ VALIDASI: Hanya broadcast jika harga masih di menit yang sama
    const priceFetchTime = new Date(currentPrice.fetchedAt)
    const nowTime = new Date(Date.now())
    const priceMinute = priceFetchTime.getHours() * 60 + priceFetchTime.getMinutes()
    const nowMinute = nowTime.getHours() * 60 + nowTime.getMinutes()
    
    if (priceMinute !== nowMinute && !isPriceStale) {
      pushLog(`PRICE | Old minute data, skip`)
      lastBroadcastedPrice = {
        buy: currentPrice.buy,
        sell: currentPrice.sell,
        fetchedAt: currentPrice.fetchedAt
      }
      return
    }
    
    // Update timestamp dan price SEBELUM broadcast dimulai
    lastBroadcastTime = now
    lastBroadcastMinute = currentMinute  // 🚫 Track menit ini sudah broadcast
    lastBroadcastedPrice = {
      buy: currentPrice.buy,
      sell: currentPrice.sell,
      fetchedAt: currentPrice.fetchedAt
    }

    // 🚀 PRE-BUILD MESSAGE untuk instant broadcast
    const broadcastData = {
      data: {
        buying_rate: currentPrice.buy,
        selling_rate: currentPrice.sell,
        updated_at: currentPrice.updated_at
      }
    }
    const message = formatMessage(broadcastData, cachedMarketData.usdIdr.rate, cachedMarketData.xauUsd, finalPriceChange, cachedMarketData.economicEvents)

    // 🚀 INSTANT BROADCAST - Langsung kirim tanpa delay
    doBroadcastInstant(message)

  } catch (e) {
    // Track error per interval
    const currentInterval = INTERVALS[currentIntervalIndex]
    intervalStats[currentInterval].errors++

    // Log error hanya sekali per 10 detik
    const now = Date.now()
    if (!global.lastErrorLog || now - global.lastErrorLog > 10000) {
      console.error(`FETCH ERROR [${currentInterval}ms] | ${e.message}`)
      global.lastErrorLog = now
    }
  } finally {
    isPriceChecking = false // Release lock
  }
}

// DISABLED: checkPriceUpdate - diganti dengan fastPoll untuk menghindari flip-flop
// setInterval(checkPriceUpdate, 100)

// ==================== CONTINUOUS FAST POLLING ====================
// Polling terus-menerus dengan 2 request paralel setiap 150ms
let isFastPolling = false
let lastKnownTimestamp = 0 // Track timestamp terbaru yang sudah di-broadcast

async function fastPoll() {
  if (isFastPolling) return
  isFastPolling = true

  try {
    // Kirim 2 request paralel untuk meningkatkan chance dapat data baru
    const results = await Promise.allSettled([
      fetchTreasury(),
      fetchTreasury()
    ])

    // Cari hasil dengan updated_at terbaru
    let newestData = null
    let newestTime = 0

    results.forEach((result) => {
      if (result.status === 'fulfilled' && result.value?.data?.updated_at) {
        const updateTime = new Date(result.value.data.updated_at).getTime()
        if (updateTime > newestTime) {
          newestTime = updateTime
          newestData = result.value
        }
      }
    })

    // CRITICAL: Skip jika data TIDAK lebih baru dari yang sudah kita punya
    if (!newestData || newestTime <= lastKnownTimestamp) {
      return
    }

    // Process the new data
    const currentPrice = {
      buy: newestData.data.buying_rate,
      sell: newestData.data.selling_rate,
      updated_at: newestData.data.updated_at,
      fetchedAt: Date.now()
    }

    // Update timestamp tracker
    lastKnownTimestamp = newestTime
    lastApiUpdateTime = newestData.data.updated_at

    if (lastKnownPrice && (lastKnownPrice.buy !== currentPrice.buy || lastKnownPrice.sell !== currentPrice.sell)) {
      const prevPrice = { ...lastKnownPrice }
      lastKnownPrice = currentPrice

      // Update daily stats & history
      if (currentPrice.buy !== prevPrice.buy) {
        await updateDailyStats(currentPrice.buy)
        await addPriceHistory(currentPrice.buy, currentPrice.sell, prevPrice.buy, prevPrice.sell, currentPrice.updated_at)
      }

      // Instant SSE broadcast
      broadcastSSE({
        type: 'price',
        buy: currentPrice.buy,
        sell: currentPrice.sell,
        prevBuy: prevPrice.buy,
        prevSell: prevPrice.sell,
        updatedAt: currentPrice.updated_at,
        usdIdr: cachedMarketData.usdIdr?.rate,
        xauUsd: cachedMarketData.xauUsd,
        serverTime: new Date().toISOString()
      })

    } else if (!lastKnownPrice) {
      // Initial price
      lastKnownPrice = currentPrice
      await updateDailyStats(currentPrice.buy)
    }
  } catch (e) {
    // Silent fail
  } finally {
    isFastPolling = false
  }
}

// Fast poll setiap 150ms (continuous)
setInterval(fastPoll, 150)

// ==================== XAU/USD REAL-TIME ====================
let lastXauUsdPrice = null
let isXauFetching = false

async function checkXauUpdate() {
  if (isXauFetching) return
  isXauFetching = true

  try {
    const price = await fetchXAUUSDFromTradingView()
    if (price && price !== lastXauUsdPrice) {
      const prevPrice = lastXauUsdPrice
      lastXauUsdPrice = price
      cachedMarketData.xauUsd = price

      // Broadcast XAU update via SSE
      broadcastSSE({
        type: 'xau',
        price: price,
        prevPrice: prevPrice,
        change: prevPrice ? (price - prevPrice).toFixed(2) : 0,
        timestamp: new Date().toISOString()
      })

    }
  } catch (e) {
    // Silent fail
  } finally {
    isXauFetching = false
  }
}

// XAU/USD polling setiap 1 detik
setInterval(checkXauUpdate, 1000)
checkXauUpdate() // Initial fetch

// ==================== STARTUP INFO ====================
console.log(`[GOLD] Bot started | Price check: ${PRICE_CHECK_INTERVAL/1000}s | Stale alert: ${STALE_PRICE_THRESHOLD/60000}min`)

const app = express()
app.use(express.json())

app.get('/', (_req, res) => {
  res.redirect('/monitoring')
})

app.get('/health', (_req, res) => {
  res.status(200).json({ 
    status: 'ok',
    timestamp: Date.now(),
    uptime: Math.floor(process.uptime()),
    ready: isReady,
    subscriptions: subscriptions.size,
    wsConnected: sock?.ws?.readyState === 1
  })
})

app.get('/qr', async (_req, res) => {
  if (!lastQr) return res.send('<pre>QR not ready</pre>')
  try {
    const mod = await import('qrcode').catch(() => null)
    if (mod?.toDataURL) {
      const dataUrl = await mod.toDataURL(lastQr, { margin: 1 })
      return res.send(`<div style="text-align:center;padding:20px"><img src="${dataUrl}" style="max-width:400px"/></div>`)
    }
  } catch (_) {}
  res.send(lastQr)
})

app.get('/stats', (_req, res) => {
  const now = Date.now()
  const timeSinceLastUpdate = lastPriceUpdateTime > 0 ? now - lastPriceUpdateTime : null
  const isPriceStale = timeSinceLastUpdate ? timeSinceLastUpdate >= STALE_PRICE_THRESHOLD : false
  
  res.json({
    status: isReady ? 'ready' : 'not_ready',
    uptime: Math.floor(process.uptime()),
    subs: subscriptions.size,
    lastPrice: lastKnownPrice,
    lastBroadcasted: lastBroadcastedPrice,
    broadcastCount: broadcastCount,
    lastBroadcastTime: lastBroadcastTime > 0 ? new Date(lastBroadcastTime).toISOString() : null,
    timeSinceLastBroadcast: lastBroadcastTime > 0 ? Math.floor((now - lastBroadcastTime) / 1000) : null,
    lastPriceUpdateTime: lastPriceUpdateTime > 0 ? new Date(lastPriceUpdateTime).toISOString() : null,
    timeSinceLastPriceUpdate: timeSinceLastUpdate ? Math.floor(timeSinceLastUpdate / 1000) : null,
    isPriceStale: isPriceStale,
    staleThreshold: STALE_PRICE_THRESHOLD / 60000,
    cachedXAUUSD: cachedXAUUSD,
    cachedEconomicEvents: cachedEconomicEvents,
    wsConnected: sock?.ws?.readyState === 1,
    logs: logs.slice(-20)
  })
})

app.get('/calendar', async (_req, res) => {
  try {
    const events = await fetchEconomicCalendar()
    res.json({
      success: true,
      count: events?.length || 0,
      events: events || [],
      formatted: formatEconomicCalendar(events)
    })
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message
    })
  }
})

// XAU/USD Proxy API - untuk menghindari CORS di frontend
app.get('/xau', async (_req, res) => {
  try {
    const price = await fetchXAUUSD()
    if (price) {
      res.json({ price, timestamp: Date.now() })
    } else {
      res.json({ price: cachedXAUUSD, timestamp: lastXAUUSDFetch, cached: true })
    }
  } catch (e) {
    res.json({ price: cachedXAUUSD, timestamp: lastXAUUSDFetch, cached: true })
  }
})

// Endpoint untuk waktu server yang akurat (WIB)
app.get('/time', (_req, res) => {
  const now = new Date()
  // Konversi ke WIB (UTC+7)
  const wibOffset = 7 * 60 * 60 * 1000
  const wibTime = new Date(now.getTime() + wibOffset + now.getTimezoneOffset() * 60 * 1000)

  res.json({
    timestamp: now.getTime(),
    iso: now.toISOString(),
    wib: wibTime.toISOString().replace('Z', '+07:00'),
    timezone: 'Asia/Jakarta'
  })
})

// Daily Stats API - konsisten di semua device (async untuk Redis)
app.get('/daily-stats', async (_req, res) => {
  const stats = await getDailyStats()
  res.json(stats)
})

// Price History API - konsisten di semua device (async untuk Redis)
app.get('/price-history', async (req, res) => {
  const page = parseInt(req.query.page) || 1
  const perPage = parseInt(req.query.perPage) || 10
  const history = await getPriceHistory(page, perPage)
  res.json(history)
})

// Clear price history (untuk reset data duplikat)
app.get('/clear-history', async (req, res) => {
  try {
    await redis.del(REDIS_KEYS.PRICE_HISTORY)
    priceHistoryCache = []
    lastAddedUpdatedAt = ''
    res.json({ success: true, message: 'Price history cleared' })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// SSE (Server-Sent Events) untuk real-time push ke frontend
const sseClients = new Set()

app.get('/sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.flushHeaders()

  // Kirim data awal
  if (lastKnownPrice) {
    res.write(`data: ${JSON.stringify({
      type: 'price',
      buy: lastKnownPrice.buy,
      sell: lastKnownPrice.sell,
      updatedAt: lastKnownPrice.updated_at,
      usdIdr: cachedMarketData.usdIdr?.rate,
      xauUsd: cachedMarketData.xauUsd
    })}\n\n`)
  }

  sseClients.add(res)

  req.on('close', () => {
    sseClients.delete(res)
  })
})

// Fungsi untuk broadcast ke semua SSE clients
function broadcastSSE(data) {
  const message = `data: ${JSON.stringify(data)}\n\n`
  sseClients.forEach(client => {
    try {
      client.write(message)
    } catch (e) {
      sseClients.delete(client)
    }
  })
}

// SSE Heartbeat - kirim ping setiap 10 detik untuk menjaga koneksi aktif
setInterval(() => {
  if (sseClients.size > 0) {
    const heartbeat = `data: ${JSON.stringify({ type: 'heartbeat', time: Date.now(), clients: sseClients.size })}\n\n`
    sseClients.forEach(client => {
      try {
        client.write(heartbeat)
      } catch (e) {
        sseClients.delete(client)
      }
    })
  }
}, 10000)

// Log status setiap 30 detik
// Status log every 30s (silent - available via /stats)

// PWA Manifest
app.get('/manifest.json', (_req, res) => {
  res.json({
    name: 'Gold Price Monitor',
    short_name: 'Gold Monitor',
    description: 'Real-time Treasury Gold Price Monitor',
    start_url: '/monitoring',
    display: 'standalone',
    background_color: '#0f1419',
    theme_color: '#f7931a',
    icons: [
      {
        src: 'https://cdn-icons-png.flaticon.com/512/2150/2150150.png',
        sizes: '192x192',
        type: 'image/png'
      },
      {
        src: 'https://cdn-icons-png.flaticon.com/512/2150/2150150.png',
        sizes: '512x512',
        type: 'image/png'
      }
    ]
  })
})

// Service Worker for PWA
app.get('/sw.js', (_req, res) => {
  res.setHeader('Content-Type', 'application/javascript')
  res.send(`
    self.addEventListener('install', (e) => {
      e.waitUntil(
        caches.open('gold-monitor-v1').then((cache) => {
          return cache.addAll(['/monitoring']);
        })
      );
    });
    self.addEventListener('fetch', (e) => {
      e.respondWith(
        caches.match(e.request).then((response) => {
          return response || fetch(e.request);
        })
      );
    });
  `)
})

// MONITORING PAGE - Professional Gold Price Dashboard
app.get('/monitoring', async (_req, res) => {
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="theme-color" content="#f7931a">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <link rel="manifest" href="/manifest.json">
  <link rel="apple-touch-icon" href="https://cdn-icons-png.flaticon.com/512/2150/2150150.png">
  <link rel="icon" type="image/png" href="https://cdn-icons-png.flaticon.com/512/2150/2150150.png">
  <title>Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, Roboto, sans-serif;
      background: #0f1419;
      min-height: 100vh;
      padding: 20px;
      color: #e7e9ea;
    }
    .container { max-width: 1100px; margin: 0 auto; }

    /* Header - Compact */
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
      padding: 10px 14px;
      background: #1a1f26;
      border-radius: 10px;
      border: 1px solid #2f3640;
    }
    .header-left h1 {
      font-size: 0.95em;
      font-weight: 600;
      color: #e7e9ea;
      margin-bottom: 2px;
    }
    .header-left .subtitle {
      font-size: 0.65em;
      color: #71767b;
    }
    .header-right {
      text-align: right;
    }
    .clock {
      font-size: 1.2em;
      font-weight: 700;
      color: #f7931a;
      font-family: 'SF Mono', 'Consolas', monospace;
      letter-spacing: 1px;
    }
    .date-info {
      font-size: 0.6em;
      color: #71767b;
      margin-top: 2px;
    }

    /* Install Button */
    .install-btn {
      display: none;
      align-items: center;
      gap: 6px;
      padding: 6px 12px;
      background: #f7931a;
      color: #000;
      border: none;
      border-radius: 6px;
      font-size: 0.75em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    .install-btn:hover {
      background: #e8850f;
      transform: scale(1.02);
    }
    .install-btn svg {
      width: 14px;
      height: 14px;
    }

    /* Stat Items */
    .stat-item {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      background: #1a1f26;
      border-radius: 6px;
      border: 1px solid #2f3640;
    }
    .stat-item .stat-label {
      font-size: 0.7em;
      color: #71767b;
      text-transform: uppercase;
    }
    .stat-item .stat-value {
      font-size: 0.85em;
      font-weight: 600;
      color: #e7e9ea;
    }
    .stat-item .stat-value.green { color: #00c853; }
    .stat-item .stat-value.blue { color: #2196f3; }
    .stat-item .stat-change {
      font-size: 0.7em;
      padding: 2px 5px;
      border-radius: 3px;
    }
    .stat-item .stat-change.up {
      color: #00c853;
      background: rgba(0, 200, 83, 0.15);
    }
    .stat-item .stat-change.down {
      color: #ff5252;
      background: rgba(255, 82, 82, 0.15);
    }
    .stat-item.price-up { border-color: #00c853; }
    .stat-item.price-up .stat-value { color: #00c853; }
    .stat-item.price-down { border-color: #ff5252; }
    .stat-item.price-down .stat-value { color: #ff5252; }
    .stat-item.invest .stat-label { color: #f7931a; }

    /* Chart Section */
    .chart-section {
      background: #1a1f26;
      border-radius: 12px;
      border: 1px solid #2f3640;
      overflow: hidden;
      margin-bottom: 24px;
    }
    .chart-header {
      padding: 12px 16px;
      border-bottom: 1px solid #2f3640;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 10px;
    }
    .chart-title {
      display: flex;
      align-items: center;
      gap: 10px;
      width: 100%;
      justify-content: center;
    }
    .chart-header h2 {
      font-size: 1em;
      font-weight: 600;
      color: #e7e9ea;
      margin: 0;
    }
    .chart-header .live-badge {
      background: #00c853;
      color: #fff;
      font-size: 0.65em;
      padding: 3px 8px;
      border-radius: 20px;
      font-weight: 600;
      text-transform: uppercase;
    }
    .chart-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      align-items: center;
      justify-content: center;
    }
    .daily-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      align-items: center;
      justify-content: center;
      padding: 8px 12px;
      background: rgba(0,0,0,0.2);
      border-top: 1px solid #2f3640;
    }
    .daily-item {
      display: flex;
      align-items: center;
      gap: 4px;
      padding: 3px 8px;
      background: #151920;
      border-radius: 4px;
      font-size: 0.7em;
    }
    .daily-item .daily-label {
      color: #71767b;
      text-transform: uppercase;
      font-size: 0.85em;
    }
    .daily-item .daily-value {
      color: #e7e9ea;
      font-weight: 600;
    }
    .daily-item .daily-value.high { color: #00c853; }
    .daily-item .daily-value.low { color: #ff5252; }
    .daily-item.sound-toggle {
      cursor: pointer;
      transition: background 0.2s;
    }
    .daily-item.sound-toggle:hover { background: #2f3640; }
    .tradingview-widget-container {
      height: 500px;
    }
    .tradingview-widget-container__widget {
      height: 100% !important;
    }

    /* History Table */
    .history-section {
      background: #1a1f26;
      border-radius: 12px;
      border: 1px solid #2f3640;
      overflow: hidden;
    }
    .history-header {
      padding: 16px 20px;
      border-bottom: 1px solid #2f3640;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .history-header h2 {
      font-size: 1em;
      font-weight: 600;
      color: #e7e9ea;
    }
    .history-header .count {
      font-size: 0.8em;
      color: #71767b;
    }
    .history-table {
      width: 100%;
      border-collapse: collapse;
    }
    .history-table th {
      text-align: left;
      padding: 12px 20px;
      font-size: 0.75em;
      color: #71767b;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      background: #15191e;
      font-weight: 600;
    }
    .history-table td {
      padding: 14px 20px;
      font-size: 0.9em;
      border-bottom: 1px solid #2f3640;
      color: #e7e9ea;
    }
    .history-table tr:last-child td {
      border-bottom: none;
    }
    .history-table tr:hover {
      background: rgba(255,255,255,0.02);
    }
    .history-table .price-up { color: #00c853; }
    .history-table .price-down { color: #ff5252; }
    .history-table .time-col { color: #71767b; font-family: monospace; }
    .history-table .no-data {
      text-align: center;
      color: #71767b;
      padding: 40px 20px;
    }
    .history-pagination {
      display: flex;
      justify-content: center;
      align-items: center;
      gap: 16px;
      padding: 16px 20px;
      border-top: 1px solid #2f3640;
    }
    .page-btn {
      background: #2f3640;
      color: #e7e9ea;
      border: none;
      padding: 8px 16px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.85em;
      transition: background 0.2s;
    }
    .page-btn:hover:not(:disabled) { background: #3d4654; }
    .page-btn:disabled { opacity: 0.4; cursor: not-allowed; }
    .page-info { color: #71767b; font-size: 0.85em; }

    /* Animations - color based on price direction */
    .price-card.updated-up {
      animation: highlight-up 0.8s ease-out 1;
    }
    .price-card.updated-up .value {
      animation: value-up 0.8s ease-out 1;
    }
    .price-card.updated-down {
      animation: highlight-down 0.8s ease-out 1;
    }
    .price-card.updated-down .value {
      animation: value-down 0.8s ease-out 1;
    }
    .updated { animation: highlight 0.3s ease-out 1; }

    @keyframes highlight-up {
      0%, 30% {
        background: linear-gradient(145deg, rgba(0, 200, 83, 0.3), rgba(0, 200, 83, 0.15));
        box-shadow: 0 0 20px rgba(0, 200, 83, 0.3);
      }
      100% {
        background: linear-gradient(145deg, #1a1f26, #151920);
        box-shadow: none;
      }
    }
    @keyframes highlight-down {
      0%, 30% {
        background: linear-gradient(145deg, rgba(255, 82, 82, 0.3), rgba(255, 82, 82, 0.15));
        box-shadow: 0 0 20px rgba(255, 82, 82, 0.3);
      }
      100% {
        background: linear-gradient(145deg, #1a1f26, #151920);
        box-shadow: none;
      }
    }
    @keyframes highlight {
      0% { background: rgba(247, 147, 26, 0.3); }
      100% { background: transparent; }
    }

    /* Responsive - Tablet */
    @media (max-width: 768px) {
      body { padding: 10px; }
      .container { max-width: 100%; }
      .header {
        flex-direction: column;
        text-align: center;
        gap: 10px;
        padding: 14px 16px;
        margin-bottom: 12px;
      }
      .header-left h1 { font-size: 1.2em; }
      .header-right { text-align: center; }
      .clock { font-size: 1.6em; }
      .chart-section { margin-bottom: 12px; border-radius: 10px; }
      .chart-header { padding: 10px 12px; gap: 8px; }
      .chart-header h2 { font-size: 0.9em; }
      .chart-stats { gap: 5px; }
      .stat-item { padding: 4px 8px; gap: 4px; }
      .stat-item .stat-label { font-size: 0.6em; }
      .stat-item .stat-value { font-size: 0.75em; }
      .stat-item .stat-change { font-size: 0.6em; padding: 1px 4px; }
      .tradingview-widget-container { height: 400px; }
      .history-section { border-radius: 10px; }
      .history-header { padding: 12px 14px; }
      .history-header h2 { font-size: 0.9em; }
      .history-table th { padding: 10px 12px; font-size: 0.7em; }
      .history-table td { padding: 10px 12px; font-size: 0.8em; }
      .history-pagination { padding: 12px; gap: 10px; }
      .page-btn { padding: 6px 12px; font-size: 0.8em; }
    }

    /* Responsive - Mobile */
    @media (max-width: 480px) {
      body { padding: 6px; }
      .header {
        padding: 10px;
        margin-bottom: 8px;
        border-radius: 8px;
      }
      .header-left h1 { font-size: 0.95em; }
      .header-left h1 svg { width: 18px; height: 18px; }
      .header-left .subtitle { font-size: 0.7em; }
      .clock { font-size: 1.3em; letter-spacing: 1px; }
      .date-info { font-size: 0.65em; }

      .chart-section {
        margin-bottom: 8px;
        border-radius: 8px;
      }
      .chart-header { padding: 8px 10px; gap: 6px; }
      .chart-title { gap: 6px; }
      .chart-header h2 { font-size: 0.8em; }
      .chart-header h2 svg { width: 12px; height: 12px; }
      .live-badge { font-size: 0.55em; padding: 2px 6px; }
      .chart-stats { gap: 4px; }
      .stat-item { padding: 3px 5px; gap: 3px; border-radius: 4px; }
      .stat-item .stat-label { font-size: 0.55em; }
      .stat-item .stat-value { font-size: 0.7em; }
      .stat-item .stat-change { font-size: 0.55em; padding: 1px 3px; border-radius: 2px; }
      .tradingview-widget-container { height: 350px; }

      .history-section { border-radius: 8px; }
      .history-header { padding: 10px 12px; }
      .history-header h2 { font-size: 0.8em; }
      .history-header h2 svg { width: 12px; height: 12px; }
      .history-table th { padding: 8px 10px; font-size: 0.6em; }
      .history-table td { padding: 8px 10px; font-size: 0.7em; }
      .history-pagination { padding: 10px; gap: 8px; flex-wrap: wrap; }
      .page-btn { padding: 5px 10px; font-size: 0.75em; }
      .page-info { font-size: 0.7em; }
    }

    /* Extra small screens */
    @media (max-width: 360px) {
      body { padding: 4px; }
      .header { padding: 8px; margin-bottom: 6px; }
      .header-left h1 { font-size: 0.85em; }
      .clock { font-size: 1.1em; }
      .chart-header { padding: 6px 8px; gap: 5px; }
      .chart-header h2 { font-size: 0.75em; }
      .stat-item { padding: 2px 4px; gap: 2px; }
      .stat-item .stat-label { font-size: 0.5em; }
      .stat-item .stat-value { font-size: 0.65em; }
      .stat-item .stat-change { font-size: 0.5em; }
      .tradingview-widget-container { height: 280px; }
      .history-table th, .history-table td { padding: 6px 8px; font-size: 0.6em; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="header-left">
        <h1><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#f7931a" stroke-width="2" style="vertical-align:middle;margin-right:10px;"><circle cx="12" cy="12" r="10"/><path d="M12 6v12M8 10h8M8 14h8"/></svg>Gold Price Monitor
        <button class="install-btn" id="installBtn" onclick="installApp()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          Install
        </button>
        </h1>
        <div class="subtitle">Real-time Treasury Gold Rates</div>
      </div>
      <div class="header-right">
        <div class="clock" id="clock">--:--:--</div>
        <div class="date-info" id="dateInfo">Loading...</div>
      </div>
    </div>

    <div class="chart-section">
      <div class="chart-header">
        <div class="chart-title">
          <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle;margin-right:8px;"><path d="M3 3v18h18"/><path d="M18 9l-5 5-4-4-3 3"/></svg>XAU/USD Chart</h2>
          <span class="live-badge">Live</span>
        </div>
        <div class="chart-stats">
          <div class="stat-item" id="buyCard">
            <span class="stat-label">Beli</span>
            <span class="stat-value" id="buyPrice">-</span>
            <span class="stat-change" id="buyChange"></span>
          </div>
          <div class="stat-item" id="sellCard">
            <span class="stat-label">Jual</span>
            <span class="stat-value" id="sellPrice">-</span>
            <span class="stat-change" id="sellChange"></span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Spread</span>
            <span class="stat-value green" id="spreadPercent">-</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">USD/IDR</span>
            <span class="stat-value blue" id="usdIdr">-</span>
          </div>
          <div class="stat-item invest">
            <span class="stat-label">20jt</span>
            <span class="stat-value" id="gram20">-</span>
            <span class="stat-change up" id="profit20">-</span>
          </div>
          <div class="stat-item invest">
            <span class="stat-label">30jt</span>
            <span class="stat-value" id="gram30">-</span>
            <span class="stat-change up" id="profit30">-</span>
          </div>
        </div>
        <!-- Daily Stats Row -->
        <div class="daily-stats">
          <div class="daily-item">
            <span class="daily-label">Open</span>
            <span class="daily-value" id="dayOpen">-</span>
          </div>
          <div class="daily-item">
            <span class="daily-label">High</span>
            <span class="daily-value high" id="dayHigh">-</span>
          </div>
          <div class="daily-item">
            <span class="daily-label">Low</span>
            <span class="daily-value low" id="dayLow">-</span>
          </div>
          <div class="daily-item">
            <span class="daily-label">Avg</span>
            <span class="daily-value" id="dayAvg">-</span>
          </div>
          <div class="daily-item">
            <span class="daily-label">Change</span>
            <span class="daily-value" id="dayChange">-</span>
          </div>
          <div class="daily-item sound-toggle" id="soundToggle" onclick="toggleSound()">
            <span class="daily-label">Sound</span>
            <span class="daily-value" id="soundStatus">ON</span>
          </div>
        </div>
      </div>
      <div class="tradingview-widget-container">
        <!-- TradingView Widget BEGIN - FULL FEATURES -->
        <div class="tradingview-widget-container__widget"></div>
        <script type="text/javascript" src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js" async>
        {
          "autosize": true,
          "height": "600",
          "symbol": "OANDA:XAUUSD",
          "interval": "1",
          "timezone": "Asia/Jakarta",
          "theme": "dark",
          "style": "1",
          "locale": "en",
          "backgroundColor": "#1a1f26",
          "gridColor": "#2f3640",
          "hide_top_toolbar": false,
          "hide_legend": false,
          "allow_symbol_change": true,
          "save_image": true,
          "calendar": true,
          "hide_volume": true,
          "hide_side_toolbar": false,
          "withdateranges": true,
          "details": false,
          "hotlist": false,
          "show_popup_button": true,
          "popup_width": "1000",
          "popup_height": "650",
          "studies": [
            "MASimple@tv-basicstudies",
            "BB@tv-basicstudies"
          ],
          "support_host": "https://www.tradingview.com"
        }
        </script>
        <!-- TradingView Widget END -->
      </div>
    </div>

    <div class="history-section">
      <div class="history-header">
        <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle;margin-right:8px;"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>Riwayat Perubahan Harga</h2>
        <span class="count" id="historyCount">0 records</span>
      </div>
      <table class="history-table">
        <thead>
          <tr>
            <th>Waktu</th>
            <th>Harga Beli</th>
            <th>Harga Jual</th>
            <th>Perubahan</th>
          </tr>
        </thead>
        <tbody id="historyBody">
          <tr><td colspan="4" class="no-data">Menunggu data...</td></tr>
        </tbody>
      </table>
      <div class="history-pagination" id="historyPagination" style="display:none;">
        <button class="page-btn" id="prevPage" disabled>Sebelumnya</button>
        <span class="page-info" id="pageInfo">Halaman 1</span>
        <button class="page-btn" id="nextPage">Selanjutnya</button>
      </div>
    </div>
  </div>

  <script>
    const days = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu'];
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];

    let lastBuy = 0;
    let lastSell = 0;
    const PER_PAGE = 10;
    let currentPage = 1;
    let totalPages = 1;
    let totalRecords = 0;

    // Load history dari server
    async function loadHistory() {
      try {
        const res = await fetch('/price-history?page=' + currentPage + '&perPage=' + PER_PAGE);
        const data = await res.json();
        totalRecords = data.total;
        totalPages = data.totalPages;
        renderServerHistory(data.items);
      } catch (e) {
        renderServerHistory([]);
      }
    }

    function renderServerHistory(items) {
      const tbody = document.getElementById('historyBody');
      const countEl = document.getElementById('historyCount');
      const pagination = document.getElementById('historyPagination');

      if (!items || items.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="no-data">Belum ada data perubahan harga</td></tr>';
        countEl.textContent = '0 records';
        pagination.style.display = 'none';
        return;
      }

      countEl.textContent = totalRecords + ' records';

      let html = '';
      items.forEach(function(item) {
        const time = new Date(item.time);
        const timeStr = time.toTimeString().substring(0, 8);
        const buyChange = item.buyChange || 0;
        const sellChange = item.sellChange || 0;
        const changeSign = buyChange >= 0 ? '+' : '';
        const changeClass = buyChange >= 0 ? 'up' : 'down';

        html += '<tr>' +
          '<td>' + timeStr + '</td>' +
          '<td>' + formatRupiah(item.buy) + '</td>' +
          '<td>' + formatRupiah(item.sell) + '</td>' +
          '<td class="' + changeClass + '">' + changeSign + buyChange.toLocaleString('id-ID') + '</td>' +
          '</tr>';
      });
      tbody.innerHTML = html;

      // Pagination
      if (totalPages > 1) {
        pagination.style.display = 'flex';
        document.getElementById('pageInfo').textContent = 'Halaman ' + currentPage + ' / ' + totalPages;
        document.getElementById('prevPage').disabled = currentPage >= totalPages;
        document.getElementById('nextPage').disabled = currentPage <= 1;
      } else {
        pagination.style.display = 'none';
      }
    }

    function prevPage() {
      if (currentPage < totalPages) {
        currentPage++;
        loadHistory();
      }
    }

    function nextPage() {
      if (currentPage > 1) {
        currentPage--;
        loadHistory();
      }
    }

    document.getElementById('prevPage').onclick = prevPage;
    document.getElementById('nextPage').onclick = nextPage;

    function formatRupiah(n) {
      return 'Rp ' + n.toLocaleString('id-ID');
    }

    function formatTime(date) {
      const h = date.getHours().toString().padStart(2, '0');
      const m = date.getMinutes().toString().padStart(2, '0');
      const s = date.getSeconds().toString().padStart(2, '0');
      return h + ':' + m + ':' + s;
    }

    // Daily Statistics - fetch dari server
    async function loadDailyStats() {
      try {
        const res = await fetch('/daily-stats');
        const data = await res.json();
        updateDailyDisplay(data);
      } catch (e) {}
    }

    function updateDailyDisplay(data) {
      if (!data) return;
      if (data.open) document.getElementById('dayOpen').textContent = formatRupiah(data.open);
      if (data.high) document.getElementById('dayHigh').textContent = formatRupiah(data.high);
      if (data.low) document.getElementById('dayLow').textContent = formatRupiah(data.low);
      if (data.avg) document.getElementById('dayAvg').textContent = formatRupiah(data.avg);

      if (data.changePct !== null) {
        const el = document.getElementById('dayChange');
        const sign = parseFloat(data.changePct) >= 0 ? '+' : '';
        el.textContent = sign + data.changePct + '%';
        el.className = 'daily-value ' + (parseFloat(data.changePct) >= 0 ? 'high' : 'low');
      }
    }

    // Refresh daily stats setiap 30 detik
    setInterval(loadDailyStats, 30000);
    loadDailyStats();

    // Sound Notification
    let soundEnabled = localStorage.getItem('soundEnabled') !== 'false';
    const notificationSound = new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdH2QkYyHh4yOkI2Mjo+Oj4mMkJGJiIiLkZCKiIePjoeGh4qMi4eCg4WLjIuGgoKGi4yKhYKBhYqLiYWDgYSIi4qGhIGDhoqKh4WDgoWHiYmHhYODhYeIiIaFhIOFhoiIh4WEg4SGh4eGhYSDhIWGhoeGhYSDhIWGhoaFhYSEhYWGhoWFhISEhYWFhYWFhISEhIWFhYWFhISDhISFhYWFhISDg4SEhYWFhYSEg4ODhISFhYWEhIODg4SEhISEhISDg4ODhISEhISEg4ODg4ODhISEhISDg4ODg4OEhISEhIODg4ODg4SEhISEg4ODg4KDg4SEhISEg4OCgoKDg4SEhISDg4KCgoKDg4OEhISDgoKCgoKDg4OEhIODgoKCgoKDg4ODg4OCgoKBgoKDg4ODg4OCgoGBgoKDg4ODg4KCgYGBgoKDg4ODgoKBgYGBgoKCg4OCgoGBgYGBgoKCgoKCgYGBgYGBgoKCgoKBgYGBgYGBgoKCgoGBgYGAgYGBgoKCgYGBgYCAgYGBgoKBgYGBgICAgYGBgYGBgYGAgICAgYGBgYGBgICAgICAgYGBgYGAgICAgICAgYGBgYCAgICAgICAgYGBgYCAgICAgICAgYGBgICAgICAgH+AgYGBgICAgIB/f4CAgYGAgICAgH9/f4CAgYCAgICAf39/f4CAgICAgIB/f39/f4CAgICAgH9/f39/f4CAgICAf39/f39/f4CAgIB/f39/f39/f4CAgIB/f39/f39/f4CAgH9/f39/f39/f4CAf39/f39/f35/f4CAf39/f39/fn5/f4B/f39/f39+fn5/f39/f39/f35+fn5/f39/f39/fn5+fn9/f39/f35+fn5+f39/f39/fn5+fn5+f39/f39+fn5+fn5+f39/f35+fn5+fn5+f39/fn5+fn5+fn5+f39/fn5+fn5+fn5+fn9/fn5+fn5+fn5+fn5/fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fn5+fX5+fn5+fn5+fn5+fX1+fn5+fn5+fn59fX5+fn5+fn5+fn19fX5+fn5+fn5+fX19fn5+fn5+fn59fX19fn5+fn5+fn19fX1+fn5+fn59fX19fX5+fn5+fn19fX19fX5+fn5+fX19fX19fn5+fn59fX19fX19fn5+fn19fX19fX19fn5+fX19fX19fX1+fn59fX19fX19fX5+fn19fX19fX19fX5+fX19fX19fX19fn59fX19fX19fX1+fn19fX19fX19fX5+fX19fX19fX19fn19fX19fX19fX1+fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fX19fXx9fX19fX19fX19fXx8fX19fX19fX18fHx9fX19fX19fHx8fH19fX19fXx8fHx8fX19fX18fHx8fHx9fX19fHx8fHx8fH19fX18fHx8fHx8fX19fHx8fHx8fHx9fX18fHx8fHx8fH19fHx8fHx8fHx8fX18fHx8fHx8fHx9fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8fHx8e3x8fHx8fHx8fHt7fHx8fHx8fHt7e3x8fHx8fHt7e3t8fHx8fHx7e3t7e3x8fHx8e3t7e3t7fHx8fHt7e3t7e3t8fHx7e3t7e3t7e3x8fHt7e3t7e3t7fHx7e3t7e3t7e3t8e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3t7e3p7e3t7e3t7e3t6ent7e3t7e3t6enp7e3t7e3t6enp6e3t7e3t7enp6ent7e3t7enp6enp7e3t7e3p6enp6e3t7e3p6enp6ent7e3p6enp6enp7e3p6enp6enp6e3t6enp6enp6ent6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6enp6');

    function toggleSound() {
      soundEnabled = !soundEnabled;
      localStorage.setItem('soundEnabled', soundEnabled);
      document.getElementById('soundStatus').textContent = soundEnabled ? 'ON' : 'OFF';
      document.getElementById('soundToggle').style.opacity = soundEnabled ? '1' : '0.5';
    }

    function playNotificationSound() {
      if (soundEnabled) {
        notificationSound.currentTime = 0;
        notificationSound.play().catch(() => {});
      }
    }

    // Update sound status on load
    document.getElementById('soundStatus').textContent = soundEnabled ? 'ON' : 'OFF';
    document.getElementById('soundToggle').style.opacity = soundEnabled ? '1' : '0.5';

    // Register Service Worker for PWA
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }

    // PWA Install Prompt
    let deferredPrompt = null;

    window.addEventListener('beforeinstallprompt', function(e) {
      e.preventDefault();
      deferredPrompt = e;
      document.getElementById('installBtn').style.display = 'inline-flex';
    });

    function installApp() {
      if (deferredPrompt) {
        deferredPrompt.prompt();
        deferredPrompt.userChoice.then(function(result) {
          if (result.outcome === 'accepted') {
            document.getElementById('installBtn').style.display = 'none';
          }
          deferredPrompt = null;
        });
      }
    }

    window.addEventListener('appinstalled', function() {
      document.getElementById('installBtn').style.display = 'none';
      deferredPrompt = null;
    });

    // Offset waktu server vs browser (dalam ms)
    let serverTimeOffset = 0;

    // Ambil waktu akurat dari server sendiri
    async function fetchServerTime() {
      try {
        const res = await fetch('/time');
        const data = await res.json();
        serverTimeOffset = data.timestamp - Date.now();
      } catch (e) {}
    }

    // Sync waktu saat load dan setiap 5 menit
    fetchServerTime();
    setInterval(fetchServerTime, 5 * 60 * 1000);

    function getAccurateTime() {
      return new Date(Date.now() + serverTimeOffset);
    }

    function updateClock() {
      const now = getAccurateTime();
      document.getElementById('clock').textContent = formatTime(now);
      const dayName = days[now.getDay()];
      const date = now.getDate();
      const month = months[now.getMonth()];
      const year = now.getFullYear();
      document.getElementById('dateInfo').textContent = dayName + ', ' + date + ' ' + month + ' ' + year + ' WIB';
    }

    // updateHistory - refresh dari server saat ada perubahan
    function updateHistory() {
      currentPage = 1; // Reset ke halaman pertama
      loadHistory();
    }

    let isFetching = false;
    let lastFetchTime = 0;
    let fetchCount = 0;

    async function fetchPrices() {
      if (isFetching) return;
      isFetching = true;
      fetchCount++;
      const fetchStart = Date.now();

      try {
        const res = await fetch('/monitoring/api', { cache: 'no-store' });
        const data = await res.json();
        const fetchTime = Date.now() - fetchStart;

        if (data.buy) {
          document.getElementById('buyPrice').textContent = formatRupiah(data.buy);
          if (data.buy !== lastBuy && lastBuy > 0) {
            const change = data.buy - lastBuy;
            const sign = change > 0 ? '+' : '';
            const cls = change > 0 ? 'up' : 'down';
            document.getElementById('buyChange').textContent = sign + change.toLocaleString('id-ID');
            document.getElementById('buyChange').className = 'change ' + cls;

            // Flash animation - remove and re-add class to trigger
            const buyCard = document.getElementById('buyCard');
            buyCard.classList.remove('updated');
            void buyCard.offsetWidth;
            buyCard.classList.add('updated');

            window.lastApiTimestamp = data.updatedAt ? new Date(data.updatedAt).getTime() : 0;
            updateHistory();
            loadDailyStats();
          }
          lastBuy = data.buy;
        }

        if (data.sell) {
          document.getElementById('sellPrice').textContent = formatRupiah(data.sell);
          if (data.sell !== lastSell && lastSell > 0) {
            const change = data.sell - lastSell;
            const sign = change > 0 ? '+' : '';
            const cls = change > 0 ? 'up' : 'down';
            document.getElementById('sellChange').textContent = sign + change.toLocaleString('id-ID');
            document.getElementById('sellChange').className = 'change ' + cls;

            // Flash animation - remove and re-add class to trigger
            const sellCard = document.getElementById('sellCard');
            sellCard.classList.remove('updated');
            void sellCard.offsetWidth; // Force reflow
            sellCard.classList.add('updated');
          }
          lastSell = data.sell;
        }

        if (data.usdIdr) {
          document.getElementById('usdIdr').textContent = 'Rp ' + Math.round(data.usdIdr).toLocaleString('id-ID');
        }
      } catch (e) {
        // Silent fail
      } finally {
        isFetching = false;
      }
    }

    setInterval(updateClock, 100);
    updateClock();

    // 🚀 SSE (Server-Sent Events) untuk real-time INSTANT update
    let evtSource = null;
    let sseReconnectTimer = null;
    let lastDataTime = Date.now();

    function connectSSE() {
      if (evtSource) {
        evtSource.close();
      }
      evtSource = new EventSource('/sse');
      setupSSEHandlers();
    }

    function setupSSEHandlers() {
    // Stats untuk evaluasi
    let updateCount = 0;
    let totalDelay = 0;
    let minDelay = Infinity;
    let maxDelay = 0;
    let delayHistory = [];

    evtSource.onmessage = function(event) {
      try {
        lastDataTime = Date.now();
        const data = JSON.parse(event.data);
        if (data.type === 'heartbeat') return;

        if (data.type === 'price') {
          // Update harga beli
          if (data.buy) {
            document.getElementById('buyPrice').textContent = formatRupiah(data.buy);

            if (data.prevBuy && data.buy !== data.prevBuy) {
              const change = data.buy - data.prevBuy;
              const sign = change > 0 ? '+' : '';
              const cls = change > 0 ? 'up' : 'down';
              document.getElementById('buyChange').textContent = sign + change.toLocaleString('id-ID');
              document.getElementById('buyChange').className = 'stat-change ' + cls;
              playNotificationSound();

              const buyCard = document.getElementById('buyCard');
              buyCard.classList.remove('updated', 'updated-up', 'updated-down', 'price-up', 'price-down');
              void buyCard.offsetWidth;
              buyCard.classList.add(change > 0 ? 'updated-up' : 'updated-down', change > 0 ? 'price-up' : 'price-down');

              updateHistory();
              loadDailyStats();
            }
            lastBuy = data.buy;
          }

          // Update harga jual
          if (data.sell) {
            document.getElementById('sellPrice').textContent = formatRupiah(data.sell);
            if (data.prevSell && data.sell !== data.prevSell) {
              const change = data.sell - data.prevSell;
              const sign = change > 0 ? '+' : '';
              const cls = change > 0 ? 'up' : 'down';
              document.getElementById('sellChange').textContent = sign + change.toLocaleString('id-ID');
              document.getElementById('sellChange').className = 'stat-change ' + cls;

              const sellCard = document.getElementById('sellCard');
              sellCard.classList.remove('updated', 'updated-up', 'updated-down', 'price-up', 'price-down');
              void sellCard.offsetWidth;
              sellCard.classList.add(change > 0 ? 'updated-up' : 'updated-down', change > 0 ? 'price-up' : 'price-down');
            }
            lastSell = data.sell;
          }

          // Update USD/IDR
          if (data.usdIdr) {
            document.getElementById('usdIdr').textContent = 'Rp ' + Math.round(data.usdIdr).toLocaleString('id-ID');
          }

          // Update Spread dan Investasi
          if (data.buy && data.sell) {
            document.getElementById('spreadPercent').textContent = ((data.sell - data.buy) / data.buy * 100).toFixed(2) + '%';

            const gram20 = 20000000 / data.buy;
            const gram30 = 30000000 / data.buy;
            const profit20 = (gram20 * data.sell) - (20000000 - 20000000 * 0.03425);
            const profit30 = (gram30 * data.sell) - (30000000 - 30000000 * 0.034);

            document.getElementById('gram20').textContent = gram20.toFixed(4) + ' gr';
            document.getElementById('gram30').textContent = gram30.toFixed(4) + ' gr';
            document.getElementById('profit20').textContent = '+Rp ' + Math.round(profit20).toLocaleString('id-ID');
            document.getElementById('profit30').textContent = '+Rp ' + Math.round(profit30).toLocaleString('id-ID');
          }
        }
      } catch (e) {}
    };

    evtSource.onopen = function() {
      const badge = document.querySelector('.live-badge');
      if (badge) { badge.textContent = 'Live'; badge.style.background = '#00c853'; }
      lastDataTime = Date.now();
    };

    evtSource.onerror = function() {
      const badge = document.querySelector('.live-badge');
      if (badge) { badge.textContent = 'Reconnecting...'; badge.style.background = '#ff9800';
      }
    };
    } // end setupSSEHandlers

    // Start SSE connection
    connectSSE();

    // Check jika tidak ada data selama 60 detik, reconnect
    setInterval(function() {
      if (Date.now() - lastDataTime > 60000) {
        connectSSE();
      }
    }, 10000);

    // Fallback: Fetch sekali saat load untuk data awal
    fetchPrices();

    // Load history dari localStorage saat halaman dimuat
    loadHistory();
  </script>
</body>
</html>`

  res.send(html)
})

// API endpoint untuk mendapatkan data monitoring (JSON) - REAL-TIME
app.get('/monitoring/api', async (_req, res) => {
  // Gunakan lastKnownPrice yang di-update oleh checkPriceUpdate setiap 1 detik
  // Ini lebih cepat daripada fetch Treasury setiap request
  let buy = lastKnownPrice?.buy || null
  let sell = lastKnownPrice?.sell || null
  let updatedAt = lastKnownPrice?.updated_at || null

  // Generate pesan real-time
  let currentMessage = ''
  if (buy && sell) {
    const priceData = {
      data: {
        buying_rate: buy,
        selling_rate: sell,
        updated_at: updatedAt
      }
    }
    currentMessage = formatMessage(priceData, cachedMarketData.usdIdr?.rate, cachedMarketData.xauUsd, null, cachedMarketData.economicEvents)
  }

  res.json({
    status: isReady ? 'ready' : 'offline',
    subscribers: subscriptions.size,
    broadcastCount,
    lastBroadcastTime: lastBroadcastTime > 0 ? new Date(lastBroadcastTime).toISOString() : null,
    timeSinceLastBroadcast: lastBroadcastTime > 0 ? Math.floor((Date.now() - lastBroadcastTime) / 1000) : null,
    usdIdr: cachedMarketData.usdIdr?.rate,
    xauUsd: cachedMarketData.xauUsd,
    buy,
    sell,
    updatedAt,
    message: currentMessage,
    logs: logs.slice(-10)
  })
})

app.listen(PORT, () => {
  console.log(`[SERVER] Ready on port ${PORT} | /monitoring | /stats | /health`)
})

// KEEP-ALIVE SYSTEM
const SELF_URL = process.env.RENDER_EXTERNAL_URL ||
                 process.env.RAILWAY_STATIC_URL ||
                 `http://localhost:${PORT}`

setInterval(async () => {
  try {
    const response = await fetch(`${SELF_URL}/health`, {
      signal: AbortSignal.timeout(5000)
    })
    
    if (response.ok) {
      const data = await response.json()
      pushLog(`PING | OK (uptime: ${Math.floor(data.uptime/60)}m, subs: ${data.subscriptions})`)
    }
  } catch (e) {
    // Silent fail
  }
}, 60 * 1000)

setTimeout(async () => {
  try {
    await fetch(`${SELF_URL}/health`, { signal: AbortSignal.timeout(5000) })
  } catch (e) {
    // Silent fail
  }
}, 30000)

async function start() {
  // Load data dari Redis saat startup
  await loadFromRedis()

  const { state, saveCreds } = await useMultiFileAuthState('./auth')
  const { version } = await fetchLatestBaileysVersion()

  sock = makeWASocket({
    version,
    logger: pino({ level: 'silent' }),
    printQRInTerminal: false,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
    },
    browser: Browsers.macOS('Desktop'),
    markOnlineOnConnect: false,
    syncFullHistory: false,
    defaultQueryTimeoutMs: 60000,
    keepAliveIntervalMs: 30000,
    getMessage: async () => ({ conversation: '' })
  })

  setInterval(() => {
    if (sock?.ws?.readyState === 1) sock.ws.ping()
  }, 30000)

  sock.ev.on('connection.update', async (u) => {
    const { connection, lastDisconnect, qr } = u
    
    if (qr) {
      lastQr = qr
      pushLog('WA | QR ready at /qr')
    }

    if (connection === 'close') {
      const reason = lastDisconnect?.error?.output?.statusCode
      pushLog(`WA | Disconnected (${reason})`)

      if (reason === DisconnectReason.loggedOut) {
        pushLog('WA | LOGGED OUT - Manual login required')
        return
      }

      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        const delay = BASE_RECONNECT_DELAY * Math.pow(1.5, reconnectAttempts)
        reconnectAttempts++
        pushLog(`WA | Reconnect ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${Math.round(delay/1000)}s`)
        setTimeout(() => start(), delay)
      } else {
        pushLog('WA | Max reconnect reached')
      }

    } else if (connection === 'open') {
      lastQr = null
      reconnectAttempts = 0
      pushLog('WA | Connected')
      pushLog('WA | Warming up 15s...')

      isReady = false
      setTimeout(async () => {
        try {
          const usdIdr = await fetchUSDIDRFromGoogle()
          cachedMarketData.usdIdr = usdIdr
          cachedMarketData.lastUsdIdrFetch = Date.now()
          pushLog(`DATA | USD/IDR: Rp ${usdIdr.rate.toLocaleString('id-ID')}`)
        } catch (e) {
          pushLog(`DATA | USD/IDR fallback`)
        }

        isReady = true
        pushLog('WA | Bot ready')
        checkPriceUpdate()

        fetchEconomicCalendar().then(events => {
          if (events && events.length > 0) {
            pushLog(`DATA | ${events.length} economic events loaded`)
          }
        })
      }, 15000)
    }
  })

  sock.ev.on('creds.update', saveCreds)

  sock.ev.on('messages.upsert', async (ev) => {
    if (!isReady || ev.type !== 'notify') return
    
    for (const msg of ev.messages) {
      try {
        if (shouldIgnoreMessage(msg)) continue

        const stanzaId = msg.key.id
        if (processedMsgIds.has(stanzaId)) continue
        processedMsgIds.add(stanzaId)

        const text = normalizeText(extractText(msg))
        if (!text) continue

        const sendTarget = msg.key.remoteJid
        
        if (/\bmulai\b|\bstart\b|\bsubscribe\b/.test(text)) {
          if (subscriptions.has(sendTarget)) {
            await sock.sendMessage(sendTarget, {
              text: '✅ Sudah aktif!\n\n📢 Update otomatis saat harga berubah\n⏰ Broadcast setiap ganti menit atau per 50 detik\n📅 Termasuk kalender ekonomi USD (auto-hide 3 jam)\n⚡ Ultra real-time (1 detik check interval)'
            }, { quoted: msg })
          } else {
            subscriptions.add(sendTarget)
            pushLog(`SUB   | ➕ ${sendTarget.substring(0, 15)}... (total: ${subscriptions.size})`)

            await sock.sendMessage(sendTarget, {
              text: '🎉 Berhasil Dimulai!\n\n📢 Notifikasi otomatis saat harga berubah\n⏰ Broadcast setiap ganti menit atau per 50 detik\n📅 Termasuk kalender ekonomi USD high-impact (auto-hide 3 jam)\n⚡ Ultra real-time (1 detik check interval)\n\n_Ketik "berhenti" untuk stop._'
            }, { quoted: msg })
          }
          continue
        }

        if (/\bberhenti\b|\bunsubscribe\b|\bstop\b/.test(text)) {
          if (subscriptions.has(sendTarget)) {
            subscriptions.delete(sendTarget)
            pushLog(`SUB   | ➖ ${sendTarget.substring(0, 15)}... (total: ${subscriptions.size})`)
            await sock.sendMessage(sendTarget, { text: '👋 Notifikasi dihentikan.' }, { quoted: msg })
          } else {
            await sock.sendMessage(sendTarget, { text: '❌ Belum aktif.' }, { quoted: msg })
          }
          continue
        }
        
        if (!/\bemas\b/.test(text)) continue

        const now = Date.now()
        const lastReply = lastReplyAtPerChat.get(sendTarget) || 0
        
        if (now - lastReply < COOLDOWN_PER_CHAT) continue
        if (now - lastGlobalReplyAt < GLOBAL_THROTTLE) continue

        try {
          await sock.sendPresenceUpdate('composing', sendTarget)
        } catch (_) {}
        
        await new Promise(r => setTimeout(r, TYPING_DURATION))

        let replyText
        try {
          const [treasury, usdIdr, xauUsd, economicEvents] = await Promise.all([
            fetchTreasury(),
            fetchUSDIDRFromGoogle(), // Only use Google Finance
            fetchXAUUSDCached(),
            fetchEconomicCalendar()
          ])
          replyText = formatMessage(treasury, usdIdr.rate, xauUsd, null, economicEvents)
        } catch (e) {
          replyText = '❌ Gagal mengambil data harga.'
        }

        await new Promise(r => setTimeout(r, 500))
        
        try {
          await sock.sendPresenceUpdate('paused', sendTarget)
        } catch (_) {}
        
        await sock.sendMessage(sendTarget, { text: replyText }, { quoted: msg })

        lastReplyAtPerChat.set(sendTarget, now)
        lastGlobalReplyAt = now
        
        await new Promise(r => setTimeout(r, 1000))
        
      } catch (e) {
        // Silent fail
      }
    }
  })
}

start().catch(e => {
  console.error('FATAL |', e.message)
  process.exit(1)
})
