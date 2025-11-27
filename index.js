// index.js - Gold Price Monitor v2.2 with Redis + Push Notifications + User Auth
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  Browsers,
  makeCacheableSignalKeyStore,
  initAuthCreds,
  proto,
  BufferJSON
} from '@whiskeysockets/baileys'
import pino from 'pino'
import express from 'express'
import http from 'http'
import https from 'https'
import { Redis } from '@upstash/redis'
import { readFileSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import webpush from 'web-push'

// VAPID Keys untuk Web Push Notifications
const VAPID_PUBLIC_KEY = 'BPvtMmw2JMUUh55UKWO9cSo014LpHor_JDQSwda_MM_J2psg3SsFhzil22utOe5o8wSsQKv218mEQbrvEwN0U18'
const VAPID_PRIVATE_KEY = 'KMp0F8Q9gzNWpRP1nBwr6xWbc__wG7LcDE17WNAuiHw'

webpush.setVapidDetails(
  'mailto:admin@goldmonitor.com',
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY
)

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

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
  PRICE_HISTORY: 'gold:price_history',
  USERS: 'gold:users',           // Hash: phone -> user data (name, expired, createdAt)
  PUSH_SUBS: 'gold:push_subs',   // Hash: phone -> push subscription JSON
  SESSIONS: 'gold:sessions',     // Hash: sessionId -> phone
  WA_GROUP_ID: 'gold:wa_group_id', // String: ID grup WA yang di-monitor
  WA_AUTH: 'gold:wa_auth',       // Hash: key -> auth data (creds, keys) for persistent WA session
  OTP_CODES: 'gold:otp_codes'    // Hash: phone -> OTP code for registration verification
}

// Admin password untuk akses admin panel
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin123'

// ID Grup WhatsApp yang membernya otomatis terdaftar (di-set via admin panel)
let monitoredGroupId = null

// ==================== REDIS AUTH STATE (Persistent WA Session) ====================
async function useRedisAuthState() {
  const writeData = async (key, data) => {
    try {
      const serialized = JSON.stringify(data, BufferJSON.replacer)
      await redis.hset(REDIS_KEYS.WA_AUTH, key, serialized)
    } catch (e) {
      console.error('Redis auth write error:', e.message)
    }
  }

  const readData = async (key) => {
    try {
      const data = await redis.hget(REDIS_KEYS.WA_AUTH, key)
      if (!data) return null
      const parsed = typeof data === 'string' ? JSON.parse(data, BufferJSON.reviver) : data
      return parsed
    } catch (e) {
      console.error('Redis auth read error:', e.message)
      return null
    }
  }

  const removeData = async (key) => {
    try {
      await redis.hdel(REDIS_KEYS.WA_AUTH, key)
    } catch (e) {
      console.error('Redis auth delete error:', e.message)
    }
  }

  // Load or initialize creds
  let creds = await readData('creds')
  if (!creds) {
    creds = initAuthCreds()
    await writeData('creds', creds)
    pushLog('WA | New credentials initialized')
  }

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {}
          for (const id of ids) {
            const value = await readData(`${type}-${id}`)
            if (value) {
              if (type === 'app-state-sync-key' && value.keyData) {
                data[id] = proto.Message.AppStateSyncKeyData.fromObject(value)
              } else {
                data[id] = value
              }
            }
          }
          return data
        },
        set: async (data) => {
          for (const [category, entries] of Object.entries(data)) {
            for (const [id, value] of Object.entries(entries || {})) {
              const key = `${category}-${id}`
              if (value) {
                await writeData(key, value)
              } else {
                await removeData(key)
              }
            }
          }
        }
      }
    },
    saveCreds: async () => {
      await writeData('creds', creds)
    }
  }
}

// Clear WA auth from Redis
async function clearRedisAuth() {
  try {
    await redis.del(REDIS_KEYS.WA_AUTH)
    pushLog('WA | Redis auth cleared')
  } catch (e) {
    pushLog('WA | Failed to clear Redis auth: ' + e.message)
  }
}

// Load grup ID dari Redis saat startup
async function loadMonitoredGroup() {
  try {
    const groupId = await redis.get(REDIS_KEYS.WA_GROUP_ID)
    if (groupId) {
      monitoredGroupId = groupId
      pushLog('WA | Monitored group: ' + groupId.substring(0, 20) + '...')
    }
  } catch (e) {
    pushLog('WA | Failed to load monitored group: ' + e.message)
  }
}

// Helper: Extract phone from JID (62xxx@s.whatsapp.net -> xxx)
function extractPhoneFromJid(jid) {
  if (!jid) return null
  const match = jid.match(/^(\d+)@/)
  if (!match) return null
  let phone = match[1]
  if (phone.startsWith('62')) phone = phone.substring(2)
  return phone
}

// Auto-register member grup ke database
async function autoRegisterGroupMember(phone, name = null) {
  if (!phone) return

  try {
    const existing = await redis.hget(REDIS_KEYS.USERS, phone)
    if (existing) return // Sudah terdaftar

    const userData = {
      name: name || 'Member ' + phone,
      createdAt: Date.now(),
      expired: null, // Default lifetime, admin bisa atur nanti
      source: 'whatsapp_group'
    }

    await redis.hset(REDIS_KEYS.USERS, phone, JSON.stringify(userData))
    pushLog('WA | Auto-registered: +62' + phone)
  } catch (e) {
    pushLog('WA | Auto-register failed: ' + e.message)
  }
}

// Remove member dari database saat keluar grup
async function removeGroupMember(phone) {
  if (!phone) return

  try {
    const existing = await redis.hget(REDIS_KEYS.USERS, phone)
    if (!existing) return

    const user = typeof existing === 'string' ? JSON.parse(existing) : existing

    // Hanya hapus jika source dari whatsapp_group
    if (user.source === 'whatsapp_group') {
      await Promise.all([
        redis.hdel(REDIS_KEYS.USERS, phone),
        redis.hdel(REDIS_KEYS.PUSH_SUBS, phone)
      ])

      // Hapus semua session user ini
      const sessions = await redis.hgetall(REDIS_KEYS.SESSIONS)
      for (const [sessId, sessPhone] of Object.entries(sessions || {})) {
        if (sessPhone === phone) {
          await redis.hdel(REDIS_KEYS.SESSIONS, sessId)
        }
      }

      pushLog('WA | Removed member: +62' + phone)
    }
  } catch (e) {
    pushLog('WA | Remove member failed: ' + e.message)
  }
}

// Cache lokal untuk mengurangi Redis calls
let dailyStatsCache = null
let priceHistoryCache = []
let lastCacheUpdate = 0
const CACHE_TTL = 5000 // 5 detik

// Load data dari Redis saat startup
async function loadFromRedis() {
  try {
    const [stats, history, lastTime] = await Promise.all([
      redis.get(REDIS_KEYS.DAILY_STATS),
      redis.lrange(REDIS_KEYS.PRICE_HISTORY, 0, -1),
      redis.get('gold:last_history_time')
    ])

    if (stats) {
      dailyStatsCache = stats
      pushLog('REDIS | Daily stats loaded')
    }

    if (history && history.length > 0) {
      priceHistoryCache = history
      pushLog(`REDIS | ${history.length} price history loaded`)
    }

    if (lastTime) {
      lastAddedUpdatedAt = lastTime
      pushLog('REDIS | Last history time loaded: ' + lastTime)
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
  // Skip jika updatedAt kosong atau sama dengan yang terakhir
  if (!updatedAt || updatedAt === lastAddedUpdatedAt) return

  // Cek lock untuk mencegah race condition
  if (isAddingHistory) return
  isAddingHistory = true

  try {
    // Cek dari Redis apakah updatedAt sudah pernah disimpan
    const lastSavedTime = await redis.get('gold:last_history_time')
    if (lastSavedTime === updatedAt) {
      lastAddedUpdatedAt = updatedAt
      isAddingHistory = false
      return
    }

    // Cek dari cache lokal
    const lastEntry = priceHistoryCache[priceHistoryCache.length - 1]
    if (lastEntry && lastEntry.time === updatedAt) {
      lastAddedUpdatedAt = updatedAt
      isAddingHistory = false
      return
    }

    const entry = {
      time: updatedAt,
      buy: buy,
      sell: sell,
      buyChange: buy - prevBuy,
      sellChange: sell - prevSell
    }

    // Simpan ke Redis (entry + timestamp terakhir)
    await Promise.all([
      redis.rpush(REDIS_KEYS.PRICE_HISTORY, entry),
      redis.set('gold:last_history_time', updatedAt)
    ])

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
      redis.del(REDIS_KEYS.PRICE_HISTORY),
      redis.del('gold:last_history_time')
    ])
    dailyStatsCache = null
    priceHistoryCache = []
    lastAddedUpdatedAt = '' // Reset supaya data baru bisa masuk
    lastKnownPrice = null // Reset supaya harga pertama hari baru dianggap initial
    lastKnownTimestamp = 0 // Reset timestamp tracker
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

      // Update daily stats & history (skip jika updatedAt sudah pernah disimpan)
      if (currentPrice.buy !== prevPrice.buy && currentPrice.updated_at !== lastAddedUpdatedAt) {
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
  res.redirect('/install-pwa')
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
  if (!lastQr) {
    const statusMsg = isReady
      ? '<span style="color:#00ff88;">✓ WhatsApp sudah terhubung!</span><br><small style="color:#71767b;">Bot aktif dan siap digunakan.</small>'
      : '<span style="color:#ffaa00;">⏳ Menunggu QR Code...</span><br><small style="color:#71767b;">Jika tidak muncul dalam 30 detik, coba Reset.</small>'

    return res.send(`
    <div style="text-align:center;padding:20px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
      <h2 style="color:#f7931a;">WhatsApp Bot Status</h2>
      <div style="margin:30px 0;padding:20px;background:#1a1f26;border-radius:12px;border:1px solid #2f3640;">
        <p style="font-size:1.2em;">${statusMsg}</p>
      </div>

      ${isReady ? `
      <div style="margin:20px 0;padding:15px;background:rgba(0,255,136,0.1);border:1px solid #00ff88;border-radius:10px;">
        <p style="color:#00ff88;margin-bottom:10px;">Bot sudah aktif!</p>
        <p style="color:#71767b;font-size:0.9em;">Jika ingin ganti nomor WA atau login ulang, klik Reset di bawah.</p>
      </div>
      ` : ''}

      <div style="margin-top:30px;">
        <a href="/qr-reset" style="display:inline-block;margin:10px;padding:12px 25px;background:#ff4444;color:white;text-decoration:none;border-radius:8px;font-weight:bold;">Reset / Login Ulang</a>
        <a href="/qr" style="display:inline-block;margin:10px;padding:12px 25px;background:#2f3640;color:white;text-decoration:none;border-radius:8px;">Refresh</a>
      </div>

      <div style="margin-top:30px;padding:15px;background:#1a1f26;border-radius:10px;text-align:left;max-width:400px;margin-left:auto;margin-right:auto;">
        <p style="color:#f7931a;font-weight:bold;margin-bottom:10px;">Jika tidak bisa "Tautkan Perangkat":</p>
        <ol style="color:#71767b;font-size:0.85em;line-height:1.8;padding-left:20px;">
          <li>Buka WhatsApp di HP</li>
          <li>Pergi ke Settings > Linked Devices</li>
          <li>Hapus semua device yang terhubung</li>
          <li>Klik "Reset / Login Ulang" di atas</li>
          <li>Scan QR code yang muncul</li>
        </ol>
      </div>

      <p style="margin-top:20px;color:#555;font-size:0.8em;">Auto-refresh dalam 10 detik...</p>
      <script>setTimeout(() => window.location.reload(), 10000);</script>
    </div>
  `)
  }

  try {
    const mod = await import('qrcode').catch(() => null)
    if (mod?.toDataURL) {
      const dataUrl = await mod.toDataURL(lastQr, { margin: 1 })
      return res.send(`
        <div style="text-align:center;padding:20px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
          <h2 style="color:#f7931a;">Scan QR dengan WhatsApp</h2>
          <div style="background:white;padding:15px;border-radius:15px;display:inline-block;margin:20px 0;">
            <img src="${dataUrl}" style="max-width:280px;display:block;"/>
          </div>
          <div style="margin:20px 0;padding:15px;background:#1a1f26;border-radius:10px;max-width:350px;margin-left:auto;margin-right:auto;">
            <p style="color:#f7931a;font-weight:bold;margin-bottom:10px;">Cara Scan:</p>
            <p style="color:#71767b;font-size:0.9em;line-height:1.6;">
              1. Buka WhatsApp di HP<br>
              2. Tap ⋮ atau Settings<br>
              3. Pilih "Linked Devices"<br>
              4. Tap "Link a Device"<br>
              5. Arahkan kamera ke QR di atas
            </p>
          </div>
          <p style="margin-top:20px;"><a href="/qr" style="color:#f7931a;">Refresh QR</a></p>
          <p style="margin-top:10px;color:#555;font-size:0.8em;">QR expires dalam 60 detik, refresh jika perlu</p>
          <script>setTimeout(() => window.location.reload(), 30000);</script>
        </div>
      `)
    }
  } catch (_) {}
  res.send(lastQr)
})

// Reset QR - Hapus session dan restart koneksi WA
app.get('/qr-reset', async (req, res) => {
  const { confirm } = req.query

  if (confirm !== 'yes') {
    return res.send(`
      <div style="text-align:center;padding:40px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
        <h2 style="color:#ff4444;">Reset WhatsApp Session</h2>
        <p style="margin:20px 0;color:#71767b;">Ini akan menghapus sesi WhatsApp dan memerlukan scan QR ulang.</p>
        <p style="margin:20px 0;color:#ffaa00;">⚠️ WhatsApp akan logout dari device ini!</p>
        <a href="/qr-reset?confirm=yes" style="display:inline-block;margin:10px;padding:15px 30px;background:#ff4444;color:white;text-decoration:none;border-radius:10px;font-weight:bold;">Ya, Reset Sekarang</a>
        <a href="/qr" style="display:inline-block;margin:10px;padding:15px 30px;background:#2f3640;color:white;text-decoration:none;border-radius:10px;">Batal</a>
      </div>
    `)
  }

  try {
    // Close existing connection
    if (sock) {
      sock.ev.removeAllListeners()
      await sock.logout().catch(() => {})
      sock = null
    }

    isReady = false
    lastQr = null

    // Clear Redis auth (persistent session)
    await clearRedisAuth()

    // Delete local auth folder if exists
    const fs = await import('fs')
    const path = await import('path')
    const authPath = path.join(process.cwd(), 'auth')

    if (fs.existsSync(authPath)) {
      fs.rmSync(authPath, { recursive: true, force: true })
      pushLog('WA | Local auth folder deleted')
    }

    // Restart connection
    pushLog('WA | Restarting connection...')
    setTimeout(() => {
      start().catch(e => pushLog('WA | Restart error: ' + e.message))
    }, 2000)

    res.send(`
      <div style="text-align:center;padding:40px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
        <h2 style="color:#00ff88;">Reset Berhasil!</h2>
        <p style="margin:20px 0;color:#71767b;">Menunggu QR code baru...</p>
        <p style="margin:20px 0;">Halaman akan refresh otomatis dalam 5 detik.</p>
        <a href="/qr" style="display:inline-block;margin:10px;padding:15px 30px;background:#f7931a;color:white;text-decoration:none;border-radius:10px;font-weight:bold;">Lihat QR Code</a>
        <script>setTimeout(() => window.location.href = '/qr', 5000);</script>
      </div>
    `)
  } catch (e) {
    pushLog('WA | Reset error: ' + e.message)
    res.send(`
      <div style="text-align:center;padding:40px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
        <h2 style="color:#ff4444;">Reset Gagal</h2>
        <p style="color:#71767b;">${e.message}</p>
        <a href="/qr" style="color:#f7931a;">Kembali</a>
      </div>
    `)
  }
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
  console.log(`[BROADCAST] Type: ${data.type}, Clients: ${sseClients.size}`)
  sseClients.forEach(client => {
    try {
      client.write(message)
    } catch (e) {
      sseClients.delete(client)
    }
  })
}

// API untuk broadcast notifikasi/promo ke semua user
// Contoh: /send-notif?title=Promo&message=Diskon%2050%25&type=promo
// type: promo, info, warning, urgent
app.get('/send-notif', (req, res) => {
  const { title, message, type = 'info' } = req.query

  if (!title || !message) {
    return res.json({ success: false, error: 'title dan message wajib diisi' })
  }

  const notifData = {
    type: 'notification',
    notifType: type, // promo, info, warning, urgent
    title: decodeURIComponent(title),
    message: decodeURIComponent(message),
    time: new Date().toISOString()
  }

  broadcastSSE(notifData)

  res.json({
    success: true,
    sent: sseClients.size,
    data: notifData
  })
})

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

// Serve icon.png dan favicon.ico
let iconBuffer = null
let faviconBuffer = null

try {
  iconBuffer = readFileSync(join(__dirname, 'icon.png'))
} catch (e) {
  console.log('Icon file not found')
}

try {
  faviconBuffer = readFileSync(join(__dirname, 'favicon.ico'))
} catch (e) {
  console.log('Favicon file not found')
}

app.get('/icon.png', (_req, res) => {
  if (iconBuffer) {
    res.setHeader('Content-Type', 'image/png')
    res.send(iconBuffer)
  } else {
    res.status(404).send('Icon not found')
  }
})

app.get('/favicon.ico', (_req, res) => {
  if (faviconBuffer) {
    res.setHeader('Content-Type', 'image/x-icon')
    res.send(faviconBuffer)
  } else if (iconBuffer) {
    res.setHeader('Content-Type', 'image/png')
    res.send(iconBuffer)
  } else {
    res.status(404).send('Favicon not found')
  }
})

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
        src: '/icon.png',
        sizes: '192x192',
        type: 'image/png',
        purpose: 'any maskable'
      },
      {
        src: '/icon.png',
        sizes: '512x512',
        type: 'image/png',
        purpose: 'any maskable'
      }
    ]
  })
})

// Service Worker for PWA - v4 dengan Push Notifications
app.get('/sw.js', (_req, res) => {
  res.setHeader('Content-Type', 'application/javascript')
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  res.send(`
    const CACHE_VERSION = 'gold-monitor-v4';

    self.addEventListener('install', (e) => {
      self.skipWaiting();
      e.waitUntil(
        caches.open(CACHE_VERSION).then((cache) => {
          return cache.addAll(['/icon.png']);
        })
      );
    });

    self.addEventListener('activate', (e) => {
      e.waitUntil(
        caches.keys().then((keys) => {
          return Promise.all(
            keys.filter(k => k !== CACHE_VERSION).map(k => caches.delete(k))
          );
        }).then(() => self.clients.claim())
      );
    });

    self.addEventListener('fetch', (e) => {
      // Jangan cache HTML - selalu fetch fresh
      if (e.request.mode === 'navigate' || e.request.url.includes('/monitoring') || e.request.url.includes('/login') || e.request.url.includes('/install')) {
        e.respondWith(fetch(e.request));
        return;
      }
      // Cache hanya untuk assets (icon, manifest)
      e.respondWith(
        caches.match(e.request).then((response) => {
          return response || fetch(e.request);
        })
      );
    });

    // Handle Push Notifications
    self.addEventListener('push', (e) => {
      let data = { title: 'Gold Price Monitor', body: 'Ada update baru!' };

      if (e.data) {
        try {
          data = e.data.json();
        } catch (err) {
          data.body = e.data.text();
        }
      }

      const options = {
        body: data.body,
        icon: data.icon || '/icon.png',
        badge: data.badge || '/icon.png',
        vibrate: [200, 100, 200],
        tag: data.type || 'notification',
        renotify: true,
        data: { url: data.url || '/monitoring' }
      };

      e.waitUntil(
        self.registration.showNotification(data.title, options)
      );
    });

    // Handle notification click
    self.addEventListener('notificationclick', (e) => {
      e.notification.close();

      const urlToOpen = e.notification.data?.url || '/monitoring';

      e.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then((windowClients) => {
          // Check if there is already a window open
          for (let client of windowClients) {
            if (client.url.includes('/monitoring') && 'focus' in client) {
              return client.focus();
            }
          }
          // If no window open, open new one
          if (clients.openWindow) {
            return clients.openWindow(urlToOpen);
          }
        })
      );
    });
  `)
})

// ADMIN PAGE - Broadcast Notifications
app.get('/admin/monitoring', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Admin - Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
      background: #0f1419;
      min-height: 100vh;
      padding: 20px;
      color: #e7e9ea;
    }
    .container { max-width: 600px; margin: 0 auto; }

    .header {
      text-align: center;
      margin-bottom: 30px;
      padding: 20px;
      background: linear-gradient(135deg, #1a1f26 0%, #2f3640 100%);
      border-radius: 15px;
      border: 1px solid #f7931a;
    }
    .header h1 {
      color: #f7931a;
      font-size: 1.5em;
      margin-bottom: 5px;
    }
    .header p { color: #71767b; font-size: 0.9em; }

    .stats-bar {
      display: flex;
      justify-content: space-around;
      background: #1a1f26;
      padding: 15px;
      border-radius: 10px;
      margin-bottom: 20px;
      border: 1px solid #2f3640;
    }
    .stat-item { text-align: center; }
    .stat-value { font-size: 1.5em; font-weight: bold; color: #f7931a; }
    .stat-label { font-size: 0.75em; color: #71767b; }

    .card {
      background: #1a1f26;
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 20px;
      border: 1px solid #2f3640;
    }
    .card h2 {
      color: #e7e9ea;
      font-size: 1.1em;
      margin-bottom: 15px;
      padding-bottom: 10px;
      border-bottom: 1px solid #2f3640;
    }

    .form-group { margin-bottom: 15px; }
    .form-group label {
      display: block;
      margin-bottom: 5px;
      color: #71767b;
      font-size: 0.85em;
    }
    .form-group input, .form-group textarea, .form-group select {
      width: 100%;
      padding: 12px;
      border: 1px solid #2f3640;
      border-radius: 8px;
      background: #0f1419;
      color: #e7e9ea;
      font-size: 1em;
    }
    .form-group input:focus, .form-group textarea:focus, .form-group select:focus {
      outline: none;
      border-color: #f7931a;
    }
    .form-group textarea { resize: vertical; min-height: 80px; }

    .type-buttons {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
    }
    .type-btn {
      padding: 10px;
      border: 2px solid #2f3640;
      border-radius: 8px;
      background: #0f1419;
      color: #71767b;
      cursor: pointer;
      text-align: center;
      transition: all 0.2s;
    }
    .type-btn:hover { border-color: #f7931a; }
    .type-btn.active { border-color: #f7931a; color: #f7931a; background: rgba(247,147,26,0.1); }
    .type-btn .icon { font-size: 1.5em; display: block; margin-bottom: 3px; }
    .type-btn .label { font-size: 0.75em; }

    .btn {
      width: 100%;
      padding: 15px;
      border: none;
      border-radius: 10px;
      font-size: 1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #ff6b00 100%);
      color: white;
    }
    .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 5px 20px rgba(247,147,26,0.3); }
    .btn-primary:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

    .result {
      margin-top: 15px;
      padding: 15px;
      border-radius: 8px;
      display: none;
    }
    .result.success { display: block; background: rgba(0,255,136,0.1); border: 1px solid #00ff88; color: #00ff88; }
    .result.error { display: block; background: rgba(255,68,68,0.1); border: 1px solid #ff4444; color: #ff4444; }

    .history { max-height: 300px; overflow-y: auto; }
    .history-item {
      padding: 12px;
      background: #0f1419;
      border-radius: 8px;
      margin-bottom: 8px;
      border-left: 3px solid #f7931a;
    }
    .history-item .time { font-size: 0.75em; color: #71767b; }
    .history-item .title { font-weight: 600; color: #e7e9ea; }
    .history-item .message { font-size: 0.9em; color: #71767b; margin-top: 3px; }
    .history-item.promo { border-left-color: #00ff88; }
    .history-item.warning { border-left-color: #ffaa00; }
    .history-item.urgent { border-left-color: #ff4444; }

    .empty-state { text-align: center; color: #71767b; padding: 30px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>Admin Panel</h1>
      <p>Gold Price Monitor - Broadcast Notifications</p>
    </div>

    <div class="stats-bar">
      <div class="stat-item">
        <div class="stat-value" id="clientCount">-</div>
        <div class="stat-label">Online Users</div>
      </div>
      <div class="stat-item">
        <div class="stat-value" id="sentCount">0</div>
        <div class="stat-label">Sent Today</div>
      </div>
    </div>

    <div class="card">
      <h2>Kirim Notifikasi</h2>
      <form id="notifForm">
        <div class="form-group">
          <label>Tipe Notifikasi</label>
          <div class="type-buttons">
            <div class="type-btn active" data-type="info">
              <span class="icon">📢</span>
              <span class="label">Info</span>
            </div>
            <div class="type-btn" data-type="promo">
              <span class="icon">🎁</span>
              <span class="label">Promo</span>
            </div>
            <div class="type-btn" data-type="warning">
              <span class="icon">⚠️</span>
              <span class="label">Warning</span>
            </div>
            <div class="type-btn" data-type="urgent">
              <span class="icon">🚨</span>
              <span class="label">Urgent</span>
            </div>
          </div>
        </div>
        <div class="form-group">
          <label>Judul</label>
          <input type="text" id="notifTitle" placeholder="Contoh: Promo Spesial!" required>
        </div>
        <div class="form-group">
          <label>Pesan</label>
          <textarea id="notifMessage" placeholder="Contoh: Dapatkan diskon 10% untuk pembelian emas hari ini!" required></textarea>
        </div>
        <button type="submit" class="btn btn-primary" id="sendBtn">
          Kirim Notifikasi
        </button>
        <div class="result" id="result"></div>
      </form>
    </div>

    <div class="card">
      <h2>Riwayat Notifikasi</h2>
      <div class="history" id="history">
        <div class="empty-state">Belum ada notifikasi dikirim</div>
      </div>
    </div>
  </div>

  <script>
    let selectedType = 'info';
    let sentCount = 0;
    const history = [];

    // Type button selection
    document.querySelectorAll('.type-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.type-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        selectedType = btn.dataset.type;
      });
    });

    // Fetch client count
    async function updateClientCount() {
      try {
        const res = await fetch('/stats');
        const data = await res.json();
        document.getElementById('clientCount').textContent = data.sseClients || 0;
      } catch(e) {
        document.getElementById('clientCount').textContent = '-';
      }
    }
    updateClientCount();
    setInterval(updateClientCount, 5000);

    // Form submit
    document.getElementById('notifForm').addEventListener('submit', async (e) => {
      e.preventDefault();

      const title = document.getElementById('notifTitle').value.trim();
      const message = document.getElementById('notifMessage').value.trim();
      const btn = document.getElementById('sendBtn');
      const result = document.getElementById('result');

      if (!title || !message) return;

      btn.disabled = true;
      btn.textContent = 'Mengirim...';

      try {
        const url = '/send-notif?title=' + encodeURIComponent(title) + '&message=' + encodeURIComponent(message) + '&type=' + selectedType;
        const res = await fetch(url);
        const data = await res.json();

        if (data.success) {
          result.className = 'result success';
          result.textContent = 'Notifikasi berhasil dikirim ke ' + data.sent + ' user!';

          // Add to history
          sentCount++;
          document.getElementById('sentCount').textContent = sentCount;
          addToHistory({ type: selectedType, title, message, time: new Date().toISOString(), sent: data.sent });

          // Reset form
          document.getElementById('notifTitle').value = '';
          document.getElementById('notifMessage').value = '';
        } else {
          result.className = 'result error';
          result.textContent = 'Gagal: ' + (data.error || 'Unknown error');
        }
      } catch(err) {
        result.className = 'result error';
        result.textContent = 'Error: ' + err.message;
      }

      btn.disabled = false;
      btn.textContent = 'Kirim Notifikasi';

      setTimeout(() => { result.className = 'result'; }, 5000);
    });

    function addToHistory(item) {
      history.unshift(item);
      renderHistory();
    }

    function renderHistory() {
      const container = document.getElementById('history');
      if (history.length === 0) {
        container.innerHTML = '<div class="empty-state">Belum ada notifikasi dikirim</div>';
        return;
      }

      container.innerHTML = history.map(item => {
        const time = new Date(item.time).toLocaleTimeString('id-ID');
        return '<div class="history-item ' + item.type + '">' +
          '<div class="time">' + time + ' - Terkirim ke ' + item.sent + ' user</div>' +
          '<div class="title">' + item.title + '</div>' +
          '<div class="message">' + item.message + '</div>' +
        '</div>';
      }).join('');
    }
  </script>
</body>
</html>`;
  res.send(html);
})

// ==================== USER AUTHENTICATION SYSTEM ====================

// Helper: Generate session ID
function generateSessionId() {
  return 'sess_' + Math.random().toString(36).substr(2, 9) + Date.now().toString(36)
}

// Helper: Normalize phone number (remove +62, 62, 0 prefix -> just numbers)
function normalizePhone(phone) {
  let clean = phone.replace(/\D/g, '')
  if (clean.startsWith('62')) clean = clean.substring(2)
  if (clean.startsWith('0')) clean = clean.substring(1)
  return clean
}

// Helper: Check if user is valid (exists and not expired)
async function isUserValid(phone) {
  try {
    const userData = await redis.hget(REDIS_KEYS.USERS, phone)
    if (!userData) return { valid: false, reason: 'not_found' }

    const user = typeof userData === 'string' ? JSON.parse(userData) : userData
    const now = Date.now()

    if (user.expired && now > user.expired) {
      return { valid: false, reason: 'expired', user }
    }

    return { valid: true, user }
  } catch (e) {
    return { valid: false, reason: 'error' }
  }
}

// API: Request OTP for registration
app.post('/api/request-otp', express.json(), async (req, res) => {
  const { phone } = req.body
  if (!phone) return res.json({ success: false, error: 'Nomor WhatsApp wajib diisi' })

  const normalizedPhone = normalizePhone(phone)

  // Check if already registered
  const existing = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)
  if (existing) {
    return res.json({ success: false, error: 'Nomor sudah terdaftar. Silakan login.' })
  }

  // Check if WhatsApp is connected
  if (!sock || !isReady) {
    return res.json({ success: false, error: 'WhatsApp tidak terhubung. Coba lagi nanti.' })
  }

  // Generate 6-digit OTP
  const otp = Math.floor(100000 + Math.random() * 900000).toString()

  // Store OTP with 5 minute expiry
  await redis.hset(REDIS_KEYS.OTP_CODES, { [normalizedPhone]: JSON.stringify({ otp, expires: Date.now() + 5 * 60 * 1000 }) })

  // Send OTP via WhatsApp
  try {
    const jid = `62${normalizedPhone}@s.whatsapp.net`
    await sock.sendMessage(jid, {
      text: `🔐 *Kode OTP Gold Price Monitor*\n\nKode verifikasi Anda: *${otp}*\n\nKode berlaku 5 menit.\nJangan bagikan kode ini kepada siapapun.`
    })

    pushLog(`OTP | Sent to +62${normalizedPhone}`)
    res.json({ success: true, message: 'Kode OTP telah dikirim ke WhatsApp Anda' })
  } catch (e) {
    pushLog(`OTP | Failed to send to +62${normalizedPhone}: ${e.message}`)
    res.json({ success: false, error: 'Gagal mengirim OTP. Pastikan nomor WhatsApp aktif.' })
  }
})

// API: Verify OTP and register user
app.post('/api/verify-otp', express.json(), async (req, res) => {
  const { phone, otp, name } = req.body
  if (!phone || !otp) return res.json({ success: false, error: 'Nomor dan OTP wajib diisi' })

  const normalizedPhone = normalizePhone(phone)

  // Get stored OTP
  const stored = await redis.hget(REDIS_KEYS.OTP_CODES, normalizedPhone)
  if (!stored) {
    return res.json({ success: false, error: 'OTP tidak ditemukan. Minta OTP baru.' })
  }

  const otpData = typeof stored === 'string' ? JSON.parse(stored) : stored

  // Check expiry
  if (Date.now() > otpData.expires) {
    await redis.hdel(REDIS_KEYS.OTP_CODES, normalizedPhone)
    return res.json({ success: false, error: 'OTP sudah expired. Minta OTP baru.' })
  }

  // Verify OTP
  if (otp !== otpData.otp) {
    return res.json({ success: false, error: 'OTP salah' })
  }

  // OTP valid - register user
  const userData = {
    name: name || 'User ' + normalizedPhone,
    createdAt: Date.now(),
    expired: null,
    source: 'otp_registration'
  }

  await redis.hset(REDIS_KEYS.USERS, { [normalizedPhone]: JSON.stringify(userData) })
  await redis.hdel(REDIS_KEYS.OTP_CODES, normalizedPhone)

  // Create session
  const sessionId = generateSessionId()
  await redis.hset(REDIS_KEYS.SESSIONS, { [sessionId]: normalizedPhone })

  pushLog(`OTP | User registered: +62${normalizedPhone}`)
  res.json({ success: true, sessionId, user: userData })
})

// API: Login user
app.post('/api/login', express.json(), async (req, res) => {
  const { phone } = req.body
  if (!phone) return res.json({ success: false, error: 'Nomor WhatsApp wajib diisi' })

  const normalizedPhone = normalizePhone(phone)
  const check = await isUserValid(normalizedPhone)

  if (!check.valid) {
    if (check.reason === 'not_found') {
      return res.json({ success: false, error: 'Nomor tidak terdaftar. Silakan daftar dulu.', needRegister: true })
    }
    if (check.reason === 'expired') {
      return res.json({ success: false, error: 'Akun sudah expired. Hubungi admin untuk perpanjang.' })
    }
    return res.json({ success: false, error: 'Terjadi kesalahan' })
  }

  // Create session
  const sessionId = generateSessionId()
  await redis.hset(REDIS_KEYS.SESSIONS, { [sessionId]: normalizedPhone })

  res.json({ success: true, sessionId, user: check.user })
})

// API: Verify session
app.get('/api/verify-session', async (req, res) => {
  const sessionId = req.query.session
  if (!sessionId) return res.json({ valid: false })

  const phone = await redis.hget(REDIS_KEYS.SESSIONS, sessionId)
  if (!phone) return res.json({ valid: false })

  const check = await isUserValid(phone)
  if (!check.valid) return res.json({ valid: false, reason: check.reason })

  res.json({ valid: true, user: check.user, phone })
})

// API: Logout
app.post('/api/logout', express.json(), async (req, res) => {
  const { session } = req.body
  if (session) {
    await redis.hdel(REDIS_KEYS.SESSIONS, session)
  }
  res.json({ success: true })
})

// API: Save push subscription
app.post('/api/push-subscribe', express.json(), async (req, res) => {
  const { session, subscription } = req.body
  if (!session || !subscription) return res.json({ success: false })

  const phone = await redis.hget(REDIS_KEYS.SESSIONS, session)
  if (!phone) return res.json({ success: false, error: 'Invalid session' })

  await redis.hset(REDIS_KEYS.PUSH_SUBS, phone, JSON.stringify(subscription))
  res.json({ success: true })
})

// API: Get VAPID public key
app.get('/api/vapid-public-key', (_req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY })
})

// ==================== ADMIN API ====================

// Admin: Get all users
app.get('/api/admin/users', async (req, res) => {
  const { password } = req.query
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  try {
    const users = await redis.hgetall(REDIS_KEYS.USERS)
    const result = []

    for (const [phone, data] of Object.entries(users || {})) {
      const user = typeof data === 'string' ? JSON.parse(data) : data
      const hasPushSub = await redis.hget(REDIS_KEYS.PUSH_SUBS, phone)
      result.push({
        phone,
        ...user,
        hasPushSubscription: !!hasPushSub
      })
    }

    res.json({ success: true, users: result })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Add user
app.post('/api/admin/users', express.json(), async (req, res) => {
  const { password, phone, name, expiredDays } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phone) return res.json({ success: false, error: 'Nomor WA wajib diisi' })

  const normalizedPhone = normalizePhone(phone)
  const now = Date.now()
  const expired = expiredDays ? now + (expiredDays * 24 * 60 * 60 * 1000) : null

  const userData = {
    name: name || 'User ' + normalizedPhone,
    createdAt: now,
    expired: expired
  }

  await redis.hset(REDIS_KEYS.USERS, normalizedPhone, JSON.stringify(userData))

  res.json({ success: true, user: { phone: normalizedPhone, ...userData } })
})

// Admin: Update user
app.put('/api/admin/users', express.json(), async (req, res) => {
  const { password, phone, name, expiredDays, addDays } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  const normalizedPhone = normalizePhone(phone)
  const existing = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)

  if (!existing) return res.json({ success: false, error: 'User tidak ditemukan' })

  const user = typeof existing === 'string' ? JSON.parse(existing) : existing

  if (name) user.name = name

  if (expiredDays !== undefined) {
    user.expired = expiredDays ? Date.now() + (expiredDays * 24 * 60 * 60 * 1000) : null
  }

  if (addDays) {
    const base = user.expired && user.expired > Date.now() ? user.expired : Date.now()
    user.expired = base + (addDays * 24 * 60 * 60 * 1000)
  }

  await redis.hset(REDIS_KEYS.USERS, normalizedPhone, JSON.stringify(user))

  res.json({ success: true, user: { phone: normalizedPhone, ...user } })
})

// Admin: Delete user
app.delete('/api/admin/users', express.json(), async (req, res) => {
  const { password, phone } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  const normalizedPhone = normalizePhone(phone)

  await Promise.all([
    redis.hdel(REDIS_KEYS.USERS, normalizedPhone),
    redis.hdel(REDIS_KEYS.PUSH_SUBS, normalizedPhone)
  ])

  // Remove all sessions for this user
  const sessions = await redis.hgetall(REDIS_KEYS.SESSIONS)
  for (const [sessId, sessPhone] of Object.entries(sessions || {})) {
    if (sessPhone === normalizedPhone) {
      await redis.hdel(REDIS_KEYS.SESSIONS, sessId)
    }
  }

  res.json({ success: true })
})

// Admin: Clear invalid users (LID format or invalid Indonesian phone numbers)
app.post('/api/admin/users/clear-invalid', express.json(), async (req, res) => {
  const { password } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  try {
    const allUsers = await redis.hgetall(REDIS_KEYS.USERS) || {}
    let deleted = 0

    for (const phone of Object.keys(allUsers)) {
      // Valid Indonesian phone: starts with 8, length 9-12 (without 62 prefix)
      // Invalid: LID numbers (very long), or doesn't start with 8
      const isValidIndonesian = /^8\d{8,11}$/.test(phone)

      if (!isValidIndonesian) {
        await redis.hdel(REDIS_KEYS.USERS, phone)
        deleted++
      }
    }

    pushLog(`Admin | Cleared ${deleted} invalid users`)
    res.json({ success: true, deleted })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Clear ALL users (use with caution!)
app.post('/api/admin/users/clear-all', express.json(), async (req, res) => {
  const { password, confirm } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })
  if (confirm !== 'DELETE_ALL') return res.json({ success: false, error: 'Konfirmasi salah' })

  try {
    await redis.del(REDIS_KEYS.USERS)
    await redis.del(REDIS_KEYS.SESSIONS)
    pushLog(`Admin | All users cleared!`)
    res.json({ success: true })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Send push notification
app.post('/api/admin/push', express.json(), async (req, res) => {
  const { password, title, message, phone, type = 'info' } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!title || !message) return res.json({ success: false, error: 'Title dan message wajib' })

  const payload = JSON.stringify({
    title,
    body: message,
    icon: '/icon.png',
    badge: '/icon.png',
    type,
    url: '/monitoring'
  })

  let sent = 0
  let failed = 0

  try {
    if (phone) {
      // Send to specific user
      const normalizedPhone = normalizePhone(phone)
      const subData = await redis.hget(REDIS_KEYS.PUSH_SUBS, normalizedPhone)
      if (subData) {
        const subscription = typeof subData === 'string' ? JSON.parse(subData) : subData
        try {
          await webpush.sendNotification(subscription, payload)
          sent++
        } catch (e) {
          failed++
          if (e.statusCode === 410) {
            await redis.hdel(REDIS_KEYS.PUSH_SUBS, normalizedPhone)
          }
        }
      }
    } else {
      // Send to all users
      const allSubs = await redis.hgetall(REDIS_KEYS.PUSH_SUBS)
      for (const [userPhone, subData] of Object.entries(allSubs || {})) {
        const subscription = typeof subData === 'string' ? JSON.parse(subData) : subData
        try {
          await webpush.sendNotification(subscription, payload)
          sent++
        } catch (e) {
          failed++
          if (e.statusCode === 410) {
            await redis.hdel(REDIS_KEYS.PUSH_SUBS, userPhone)
          }
        }
      }
    }

    // Also broadcast via SSE
    broadcastSSE({ type: 'notification', notifType: type, title, message, time: new Date().toISOString() })

    res.json({ success: true, sent, failed })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// ==================== WHATSAPP GROUP MANAGEMENT ====================

// Admin: Get list of WhatsApp groups
app.get('/api/admin/wa-groups', async (req, res) => {
  const { password } = req.query
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!sock || !isReady) {
    return res.json({ success: false, error: 'WhatsApp not connected' })
  }

  try {
    const groups = await sock.groupFetchAllParticipating()
    const groupList = Object.values(groups).map(g => ({
      id: g.id,
      name: g.subject,
      participants: g.participants?.length || 0,
      isMonitored: g.id === monitoredGroupId
    }))

    res.json({ success: true, groups: groupList, currentGroupId: monitoredGroupId })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Set monitored group
app.post('/api/admin/wa-groups/set', express.json(), async (req, res) => {
  const { password, groupId } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!groupId) return res.json({ success: false, error: 'Group ID wajib' })

  try {
    await redis.set(REDIS_KEYS.WA_GROUP_ID, groupId)
    monitoredGroupId = groupId
    pushLog('WA | Monitored group set: ' + groupId.substring(0, 20) + '...')

    res.json({ success: true, groupId })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Debug - get group members raw data
app.get('/api/admin/wa-groups/debug', async (req, res) => {
  const { password } = req.query
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!sock || !isReady) {
    return res.json({ success: false, error: 'WhatsApp not connected' })
  }

  if (!monitoredGroupId) {
    return res.json({ success: false, error: 'Belum ada grup yang dipilih' })
  }

  try {
    const groupMeta = await sock.groupMetadata(monitoredGroupId)
    const participants = groupMeta.participants || []

    // Try to get phone numbers using lidToPhone mapping if available
    const sampleWithPhone = []
    for (const p of participants.slice(0, 10)) {
      let phoneNumber = null

      // Check if it's LID format (@lid) or standard format (@s.whatsapp.net)
      if (p.id.endsWith('@lid')) {
        // Try to resolve LID to phone number
        try {
          // Check if sock has lidToPhone store
          if (sock.store?.lidToPhone) {
            phoneNumber = sock.store.lidToPhone.get(p.id)
          }
        } catch (e) {}
      } else if (p.id.endsWith('@s.whatsapp.net')) {
        // Standard format - extract phone directly
        const match = p.id.match(/^(\d+)@/)
        if (match) phoneNumber = match[1]
      }

      sampleWithPhone.push({
        id: p.id,
        admin: p.admin,
        notify: p.notify,
        resolvedPhone: phoneNumber
      })
    }

    res.json({
      success: true,
      groupId: monitoredGroupId,
      groupName: groupMeta.subject,
      totalParticipants: participants.length,
      sampleParticipants: sampleWithPhone,
      note: 'WhatsApp menggunakan LID (Linked ID) untuk privacy. Nomor asli mungkin tidak bisa diakses.'
    })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Sync all members from monitored group
// NOTE: WhatsApp now uses LID (Linked ID) format which doesn't expose phone numbers
// This function will inform admin about this limitation
app.post('/api/admin/wa-groups/sync', express.json(), async (req, res) => {
  const { password } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!sock || !isReady) {
    return res.json({ success: false, error: 'WhatsApp not connected' })
  }

  if (!monitoredGroupId) {
    return res.json({ success: false, error: 'Belum ada grup yang dipilih' })
  }

  try {
    const groupMeta = await sock.groupMetadata(monitoredGroupId)
    const participants = groupMeta.participants || []

    pushLog(`WA | Checking ${participants.length} members from group`)

    // Check if participants use LID format
    const usesLid = participants.some(p => p.id?.endsWith('@lid'))

    if (usesLid) {
      pushLog(`WA | Group uses LID format - phone numbers hidden by WhatsApp`)
      return res.json({
        success: false,
        error: 'WhatsApp menggunakan format LID (privacy) di grup ini. Nomor telepon tidak dapat diakses otomatis. Gunakan fitur "Tambah User Manual" atau aktifkan "Registrasi via OTP".',
        total: participants.length,
        usesLid: true
      })
    }

    // Standard format - proceed with sync
    const existingUsers = await redis.hgetall(REDIS_KEYS.USERS) || {}

    let added = 0
    let skipped = 0
    let errors = 0

    for (const p of participants) {
      if (!p.id) continue

      const jidMatch = p.id.match(/^(\d+)@s\.whatsapp\.net/)
      if (!jidMatch) continue

      const fullPhone = jidMatch[1]
      const phone = fullPhone.startsWith('62') ? fullPhone.substring(2) : fullPhone

      if (!phone || phone.length < 9) continue

      if (existingUsers[phone]) {
        skipped++
        continue
      }

      try {
        const userData = JSON.stringify({
          name: p.notify || p.verifiedName || 'Member ' + phone,
          createdAt: Date.now(),
          expired: null,
          source: 'whatsapp_group'
        })

        await redis.hset(REDIS_KEYS.USERS, { [phone]: userData })
        added++
      } catch (err) {
        errors++
      }
    }

    pushLog(`WA | Sync completed: ${added} added, ${skipped} skipped`)
    res.json({ success: true, added, skipped, errors, total: participants.length })
  } catch (e) {
    pushLog(`WA | Sync error: ${e.message}`)
    res.json({ success: false, error: e.message })
  }
})

// ==================== LOGIN PAGE ====================
app.get('/login', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
  <meta name="theme-color" content="#0f1419">
  <link rel="manifest" href="/manifest.json">
  <link rel="icon" href="/icon.png">
  <title>Login - Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
      background: linear-gradient(135deg, #0f1419 0%, #1a1f26 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      color: #e7e9ea;
    }
    .login-container {
      width: 100%;
      max-width: 400px;
    }
    .login-card {
      background: rgba(26, 31, 38, 0.95);
      border-radius: 20px;
      padding: 40px 30px;
      border: 1px solid #2f3640;
      box-shadow: 0 20px 60px rgba(0,0,0,0.5);
    }
    .logo {
      text-align: center;
      margin-bottom: 30px;
    }
    .logo img {
      width: 80px;
      height: 80px;
      border-radius: 20px;
    }
    .logo h1 {
      color: #f7931a;
      font-size: 1.5em;
      margin-top: 15px;
    }
    .logo p {
      color: #71767b;
      font-size: 0.9em;
      margin-top: 5px;
    }
    .form-group {
      margin-bottom: 20px;
    }
    .form-group label {
      display: block;
      color: #71767b;
      font-size: 0.85em;
      margin-bottom: 8px;
    }
    .input-wrapper {
      position: relative;
    }
    .input-wrapper .prefix {
      position: absolute;
      left: 15px;
      top: 50%;
      transform: translateY(-50%);
      color: #71767b;
      font-size: 1em;
    }
    .form-group input {
      width: 100%;
      padding: 15px 15px 15px 50px;
      border: 2px solid #2f3640;
      border-radius: 12px;
      background: #0f1419;
      color: #e7e9ea;
      font-size: 1.1em;
      transition: border-color 0.2s;
    }
    .form-group input:focus {
      outline: none;
      border-color: #f7931a;
    }
    .form-group input::placeholder {
      color: #555;
    }
    .btn {
      width: 100%;
      padding: 15px;
      border: none;
      border-radius: 12px;
      font-size: 1.1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #ff6b00 100%);
      color: white;
    }
    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 10px 30px rgba(247,147,26,0.3);
    }
    .btn-primary:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }
    .error-msg {
      background: rgba(255,68,68,0.1);
      border: 1px solid #ff4444;
      color: #ff4444;
      padding: 12px;
      border-radius: 10px;
      margin-bottom: 20px;
      font-size: 0.9em;
      display: none;
    }
    .error-msg.show { display: block; }
    .footer-text {
      text-align: center;
      margin-top: 20px;
      color: #71767b;
      font-size: 0.8em;
    }
  </style>
</head>
<body>
  <div class="login-container">
    <div class="login-card">
      <div class="logo">
        <img src="/icon.png" alt="Gold Monitor">
        <h1>Gold Price Monitor</h1>
        <p>Masuk dengan nomor WhatsApp</p>
      </div>

      <div class="error-msg" id="errorMsg"></div>

      <form id="loginForm">
        <div class="form-group">
          <label>Nomor WhatsApp</label>
          <div class="input-wrapper">
            <span class="prefix">+62</span>
            <input type="tel" id="phone" placeholder="812xxxxxxxx" required pattern="[0-9]{9,13}" inputmode="numeric">
          </div>
        </div>
        <button type="submit" class="btn btn-primary" id="loginBtn">Masuk</button>
      </form>

      <p class="footer-text">Hubungi admin jika belum punya akun</p>
      <p class="footer-text" style="margin-top:10px;font-size:0.75em;">Untuk admin: <a href="/admin/users" style="color:#f7931a;">kelola user</a></p>
    </div>
  </div>

  <script>
    // Check if PWA is installed first
    const pwaInstalled = localStorage.getItem('pwa_installed');
    const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;

    if (!pwaInstalled && !isStandalone) {
      // Belum install PWA, redirect ke halaman install
      window.location.href = '/install-pwa';
    }

    // Check existing session
    const existingSession = localStorage.getItem('goldmonitor_session');
    if (existingSession) {
      fetch('/api/verify-session?session=' + existingSession)
        .then(r => r.json())
        .then(data => {
          if (data.valid) {
            window.location.href = '/monitoring';
          }
        });
    }

    document.getElementById('loginForm').addEventListener('submit', async (e) => {
      e.preventDefault();

      const phone = document.getElementById('phone').value.trim();
      const btn = document.getElementById('loginBtn');
      const errorMsg = document.getElementById('errorMsg');

      btn.disabled = true;
      btn.textContent = 'Memproses...';
      errorMsg.classList.remove('show');

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ phone: '62' + phone })
        });
        const data = await res.json();

        if (data.success) {
          localStorage.setItem('goldmonitor_session', data.sessionId);
          localStorage.setItem('goldmonitor_user', JSON.stringify(data.user));
          window.location.href = '/monitoring';
        } else {
          errorMsg.textContent = data.error;
          errorMsg.classList.add('show');
        }
      } catch (err) {
        errorMsg.textContent = 'Terjadi kesalahan. Coba lagi.';
        errorMsg.classList.add('show');
      }

      btn.disabled = false;
      btn.textContent = 'Masuk';
    });
  </script>
</body>
</html>`;
  res.send(html);
})

// ==================== INSTALL PWA PAGE ====================
app.get('/install-pwa', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
  <meta name="theme-color" content="#0f1419">
  <link rel="manifest" href="/manifest.json">
  <link rel="icon" href="/icon.png">
  <title>Install App - Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
      background: linear-gradient(135deg, #0f1419 0%, #1a1f26 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      color: #e7e9ea;
    }
    .container {
      width: 100%;
      max-width: 450px;
      text-align: center;
    }
    .card {
      background: rgba(26, 31, 38, 0.95);
      border-radius: 20px;
      padding: 40px 30px;
      border: 1px solid #2f3640;
      box-shadow: 0 20px 60px rgba(0,0,0,0.5);
    }
    .icon {
      width: 100px;
      height: 100px;
      margin: 0 auto 25px;
      border-radius: 25px;
      overflow: hidden;
      box-shadow: 0 10px 30px rgba(247,147,26,0.3);
    }
    .icon img { width: 100%; height: 100%; }
    h1 {
      color: #f7931a;
      font-size: 1.5em;
      margin-bottom: 10px;
    }
    .subtitle {
      color: #71767b;
      font-size: 0.95em;
      margin-bottom: 30px;
      line-height: 1.5;
    }
    .steps {
      text-align: left;
      margin-bottom: 30px;
    }
    .step {
      display: flex;
      align-items: flex-start;
      gap: 15px;
      padding: 15px;
      background: #0f1419;
      border-radius: 12px;
      margin-bottom: 10px;
    }
    .step-num {
      width: 30px;
      height: 30px;
      background: linear-gradient(135deg, #f7931a 0%, #ff6b00 100%);
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: bold;
      font-size: 0.9em;
      flex-shrink: 0;
    }
    .step-text {
      color: #e7e9ea;
      font-size: 0.9em;
      line-height: 1.4;
    }
    .step-text strong { color: #f7931a; }
    .btn {
      width: 100%;
      padding: 15px;
      border: none;
      border-radius: 12px;
      font-size: 1.1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
      margin-bottom: 10px;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #ff6b00 100%);
      color: white;
    }
    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 10px 30px rgba(247,147,26,0.3);
    }
    .btn-secondary {
      background: #2f3640;
      color: #e7e9ea;
    }
    .btn-secondary:hover {
      background: #3f4650;
    }
    .installed-msg {
      display: none;
      background: rgba(0,255,136,0.1);
      border: 1px solid #00ff88;
      color: #00ff88;
      padding: 15px;
      border-radius: 12px;
      margin-bottom: 20px;
    }
    .installed-msg.show { display: block; }
    .skip-link {
      color: #71767b;
      font-size: 0.85em;
      margin-top: 15px;
      display: none;
    }
    .skip-link a { color: #f7931a; text-decoration: none; }
    .android-steps, .ios-steps { display: none; }
    .android-steps.show, .ios-steps.show { display: block; }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <div class="icon">
        <img src="/icon.png" alt="Gold Monitor">
      </div>
      <h1>Install Aplikasi</h1>
      <p class="subtitle">Install Gold Price Monitor untuk pengalaman terbaik dan menerima notifikasi harga emas real-time</p>

      <div class="installed-msg" id="installedMsg">
        Aplikasi sudah terinstall! Klik tombol di bawah untuk lanjut.
      </div>

      <div class="steps android-steps" id="androidSteps">
        <div class="step">
          <div class="step-num">1</div>
          <div class="step-text">Tap tombol <strong>"Install Aplikasi"</strong> di bawah</div>
        </div>
        <div class="step">
          <div class="step-num">2</div>
          <div class="step-text">Pilih <strong>"Add to Home Screen"</strong> atau <strong>"Install"</strong></div>
        </div>
        <div class="step">
          <div class="step-num">3</div>
          <div class="step-text">Buka aplikasi dari home screen</div>
        </div>
      </div>

      <div class="steps ios-steps" id="iosSteps">
        <div class="step">
          <div class="step-num">1</div>
          <div class="step-text">Tap tombol <strong>Share</strong> di browser (ikon kotak dengan panah)</div>
        </div>
        <div class="step">
          <div class="step-num">2</div>
          <div class="step-text">Scroll dan pilih <strong>"Add to Home Screen"</strong></div>
        </div>
        <div class="step">
          <div class="step-num">3</div>
          <div class="step-text">Tap <strong>"Add"</strong> untuk konfirmasi</div>
        </div>
      </div>

      <button class="btn btn-primary" id="installBtn">Install Aplikasi</button>
      <button class="btn btn-secondary" id="continueBtn" style="display:none;">Lanjut Login</button>
    </div>
  </div>

  <script>
    let deferredPrompt;
    const installBtn = document.getElementById('installBtn');
    const continueBtn = document.getElementById('continueBtn');
    const installedMsg = document.getElementById('installedMsg');

    // Detect iOS
    const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent);
    const isAndroid = /Android/.test(navigator.userAgent);
    const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;

    // Mark as installed in localStorage
    function markInstalled() {
      localStorage.setItem('pwa_installed', 'true');
      installedMsg.classList.add('show');
      installBtn.style.display = 'none';
      continueBtn.style.display = 'block';
      continueBtn.onclick = () => window.location.href = '/login';
    }

    if (isStandalone) {
      // Already running as PWA
      markInstalled();
    } else if (localStorage.getItem('pwa_installed') === 'true') {
      // Previously installed
      installedMsg.textContent = 'Aplikasi sudah terinstall! Buka dari home screen untuk pengalaman terbaik.';
      markInstalled();
    } else if (isIOS) {
      document.getElementById('iosSteps').classList.add('show');
      installBtn.textContent = 'Panduan Install iOS';
      installBtn.onclick = () => {
        alert('1. Tap tombol Share di browser Safari\\n2. Scroll dan pilih "Add to Home Screen"\\n3. Tap "Add"\\n\\nSetelah install, buka aplikasi dari home screen.');
      };
      // Show continue button for iOS after some time
      setTimeout(() => {
        continueBtn.style.display = 'block';
        continueBtn.textContent = 'Sudah Install? Lanjut Login';
        continueBtn.onclick = () => {
          localStorage.setItem('pwa_installed', 'true');
          window.location.href = '/login';
        };
      }, 3000);
    } else {
      document.getElementById('androidSteps').classList.add('show');

      window.addEventListener('beforeinstallprompt', (e) => {
        e.preventDefault();
        deferredPrompt = e;
        installBtn.style.display = 'block';
      });

      installBtn.onclick = async () => {
        if (deferredPrompt) {
          deferredPrompt.prompt();
          const { outcome } = await deferredPrompt.userChoice;
          if (outcome === 'accepted') {
            markInstalled();
          }
          deferredPrompt = null;
        } else {
          alert('Gunakan Chrome/Edge untuk install, atau pilih menu (titik 3) > "Add to Home Screen" / "Install App"');
        }
      };

      // Show continue button after some time for users who can't install
      setTimeout(() => {
        if (!localStorage.getItem('pwa_installed')) {
          continueBtn.style.display = 'block';
          continueBtn.textContent = 'Sudah Install? Lanjut Login';
          continueBtn.onclick = () => {
            localStorage.setItem('pwa_installed', 'true');
            window.location.href = '/login';
          };
        }
      }, 5000);
    }

    window.addEventListener('appinstalled', () => {
      markInstalled();
    });

    // Register Service Worker
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }
  </script>
</body>
</html>`;
  res.send(html);
})

// ==================== ADMIN PANEL - USER MANAGEMENT ====================
app.get('/admin/users', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Admin - Kelola User</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
      background: #0f1419;
      min-height: 100vh;
      padding: 20px;
      color: #e7e9ea;
    }
    .container { max-width: 900px; margin: 0 auto; }

    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
      padding: 15px 20px;
      background: #1a1f26;
      border-radius: 12px;
      border: 1px solid #2f3640;
    }
    .header h1 { color: #f7931a; font-size: 1.3em; }
    .header-actions { display: flex; gap: 10px; }
    .header-actions a {
      padding: 8px 15px;
      background: #2f3640;
      color: #e7e9ea;
      text-decoration: none;
      border-radius: 8px;
      font-size: 0.85em;
    }
    .header-actions a:hover { background: #3f4650; }

    .login-form {
      background: #1a1f26;
      padding: 30px;
      border-radius: 12px;
      border: 1px solid #2f3640;
      max-width: 400px;
      margin: 50px auto;
    }
    .login-form h2 { text-align: center; margin-bottom: 20px; color: #f7931a; }

    .card {
      background: #1a1f26;
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 20px;
      border: 1px solid #2f3640;
    }
    .card h2 {
      color: #e7e9ea;
      font-size: 1.1em;
      margin-bottom: 15px;
      padding-bottom: 10px;
      border-bottom: 1px solid #2f3640;
    }

    .form-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 10px;
      margin-bottom: 15px;
    }
    .form-group { margin-bottom: 10px; }
    .form-group label {
      display: block;
      margin-bottom: 5px;
      color: #71767b;
      font-size: 0.85em;
    }
    .form-group input, .form-group select {
      width: 100%;
      padding: 10px;
      border: 1px solid #2f3640;
      border-radius: 8px;
      background: #0f1419;
      color: #e7e9ea;
      font-size: 0.95em;
    }
    .form-group input:focus { outline: none; border-color: #f7931a; }

    .btn {
      padding: 10px 20px;
      border: none;
      border-radius: 8px;
      font-size: 0.95em;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #ff6b00 100%);
      color: white;
    }
    .btn-primary:hover { transform: translateY(-1px); }
    .btn-danger { background: #ff4444; color: white; }
    .btn-danger:hover { background: #ff6666; }
    .btn-sm { padding: 6px 12px; font-size: 0.8em; }

    .user-table {
      width: 100%;
      border-collapse: collapse;
    }
    .user-table th, .user-table td {
      padding: 12px 10px;
      text-align: left;
      border-bottom: 1px solid #2f3640;
    }
    .user-table th {
      color: #71767b;
      font-size: 0.8em;
      text-transform: uppercase;
    }
    .user-table tr:hover { background: rgba(247,147,26,0.05); }

    .status-badge {
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 0.75em;
      font-weight: 600;
    }
    .status-active { background: rgba(0,255,136,0.2); color: #00ff88; }
    .status-expired { background: rgba(255,68,68,0.2); color: #ff4444; }
    .status-lifetime { background: rgba(247,147,26,0.2); color: #f7931a; }

    .push-badge {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
    }
    .push-yes { background: #00ff88; }
    .push-no { background: #ff4444; }

    .stats-row {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 15px;
      margin-bottom: 20px;
    }
    .stat-card {
      background: #1a1f26;
      padding: 20px;
      border-radius: 12px;
      text-align: center;
      border: 1px solid #2f3640;
    }
    .stat-value { font-size: 2em; font-weight: bold; color: #f7931a; }
    .stat-label { color: #71767b; font-size: 0.85em; margin-top: 5px; }

    .modal {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.8);
      align-items: center;
      justify-content: center;
      z-index: 1000;
    }
    .modal.show { display: flex; }
    .modal-content {
      background: #1a1f26;
      padding: 25px;
      border-radius: 15px;
      width: 90%;
      max-width: 400px;
      border: 1px solid #2f3640;
    }
    .modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
    }
    .modal-header h3 { color: #f7931a; }
    .modal-close {
      background: none;
      border: none;
      color: #71767b;
      font-size: 1.5em;
      cursor: pointer;
    }

    .empty-state {
      text-align: center;
      padding: 40px;
      color: #71767b;
    }

    .result-msg {
      padding: 10px 15px;
      border-radius: 8px;
      margin-bottom: 15px;
      display: none;
    }
    .result-msg.success { display: block; background: rgba(0,255,136,0.1); border: 1px solid #00ff88; color: #00ff88; }
    .result-msg.error { display: block; background: rgba(255,68,68,0.1); border: 1px solid #ff4444; color: #ff4444; }

    @media (max-width: 600px) {
      .user-table { font-size: 0.85em; }
      .user-table th, .user-table td { padding: 8px 5px; }
      .stats-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Login Form -->
    <div class="login-form" id="loginForm">
      <h2>Admin Login</h2>
      <div class="form-group">
        <label>Password Admin</label>
        <input type="password" id="adminPassword" placeholder="Masukkan password">
      </div>
      <button class="btn btn-primary" style="width:100%;margin-top:10px;" onclick="adminLogin()">Login</button>
    </div>

    <!-- Main Content (hidden until login) -->
    <div id="mainContent" style="display:none;">
      <div class="header">
        <h1>Kelola User</h1>
        <div class="header-actions">
          <a href="/admin/monitoring">Notifikasi</a>
          <a href="/monitoring" target="_blank">Monitoring</a>
        </div>
      </div>

      <div class="stats-row">
        <div class="stat-card">
          <div class="stat-value" id="totalUsers">0</div>
          <div class="stat-label">Total User</div>
        </div>
        <div class="stat-card">
          <div class="stat-value" id="activeUsers">0</div>
          <div class="stat-label">User Aktif</div>
        </div>
        <div class="stat-card">
          <div class="stat-value" id="pushUsers">0</div>
          <div class="stat-label">Push Enabled</div>
        </div>
      </div>

      <!-- WhatsApp Group Sync -->
      <div class="card">
        <h2>Sinkronisasi Grup WhatsApp</h2>
        <p style="color:#71767b;font-size:0.85em;margin-bottom:15px;">Member grup yang dipilih akan otomatis terdaftar dan bisa login ke website.</p>
        <div class="result-msg" id="syncResult"></div>
        <div class="form-row" style="align-items:flex-end;">
          <div class="form-group" style="flex:2;">
            <label>Pilih Grup WhatsApp</label>
            <select id="waGroupSelect" style="width:100%;padding:10px;border:1px solid #2f3640;border-radius:8px;background:#0f1419;color:#e7e9ea;">
              <option value="">-- Pilih Grup --</option>
            </select>
          </div>
          <div class="form-group" style="flex:1;">
            <button class="btn btn-primary" onclick="setWaGroup()" style="width:100%;">Set Grup</button>
          </div>
        </div>
        <div style="display:flex;gap:10px;margin-top:10px;flex-wrap:wrap;">
          <button class="btn" style="background:#2f3640;color:#e7e9ea;" onclick="loadWaGroups()">Refresh Grup</button>
          <button class="btn" style="background:#ff4444;color:white;" onclick="clearInvalidUsers()">Hapus User Invalid</button>
          <button class="btn" style="background:#880000;color:white;" onclick="clearAllUsers()">Hapus Semua User</button>
        </div>
        <div id="currentGroup" style="margin-top:10px;font-size:0.85em;color:#71767b;"></div>
        <div style="margin-top:15px;padding:12px;background:rgba(255,170,0,0.1);border:1px solid #ffaa00;border-radius:8px;">
          <p style="color:#ffaa00;font-size:0.85em;margin:0;">
            <strong>⚠️ Catatan:</strong> WhatsApp menggunakan format LID (privacy) sehingga nomor telepon member tidak bisa diakses otomatis.
            User harus mendaftar sendiri via OTP atau ditambahkan manual oleh admin.
          </p>
        </div>
      </div>

      <div class="card">
        <h2>Tambah User Manual</h2>
        <div class="result-msg" id="addResult"></div>
        <div class="form-row">
          <div class="form-group">
            <label>Nomor WhatsApp</label>
            <input type="tel" id="newPhone" placeholder="08123456789">
          </div>
          <div class="form-group">
            <label>Nama (opsional)</label>
            <input type="text" id="newName" placeholder="Nama user">
          </div>
          <div class="form-group">
            <label>Expired (hari)</label>
            <input type="number" id="newExpired" placeholder="30" min="0">
          </div>
        </div>
        <button class="btn btn-primary" onclick="addUser()">Tambah User</button>
        <small style="color:#71767b;margin-left:10px;">Kosongkan expired untuk lifetime</small>
      </div>

      <div class="card">
        <h2>Daftar User</h2>
        <table class="user-table">
          <thead>
            <tr>
              <th>No WA</th>
              <th>Nama</th>
              <th>Status</th>
              <th>Push</th>
              <th>Expired</th>
              <th>Aksi</th>
            </tr>
          </thead>
          <tbody id="userList">
            <tr><td colspan="6" class="empty-state">Memuat data...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Edit Modal -->
  <div class="modal" id="editModal">
    <div class="modal-content">
      <div class="modal-header">
        <h3>Edit User</h3>
        <button class="modal-close" onclick="closeModal()">&times;</button>
      </div>
      <div class="form-group">
        <label>Nomor WhatsApp</label>
        <input type="text" id="editPhone" readonly style="opacity:0.7;">
      </div>
      <div class="form-group">
        <label>Nama</label>
        <input type="text" id="editName">
      </div>
      <div class="form-group">
        <label>Tambah Hari</label>
        <input type="number" id="editAddDays" placeholder="30" min="0">
      </div>
      <button class="btn btn-primary" style="width:100%;margin-top:15px;" onclick="saveUser()">Simpan</button>
    </div>
  </div>

  <!-- Push Modal -->
  <div class="modal" id="pushModal">
    <div class="modal-content">
      <div class="modal-header">
        <h3>Kirim Notifikasi</h3>
        <button class="modal-close" onclick="closePushModal()">&times;</button>
      </div>
      <input type="hidden" id="pushPhone">
      <div class="form-group">
        <label>Tipe</label>
        <select id="pushType">
          <option value="info">Info</option>
          <option value="promo">Promo</option>
          <option value="warning">Warning</option>
          <option value="urgent">Urgent</option>
        </select>
      </div>
      <div class="form-group">
        <label>Judul</label>
        <input type="text" id="pushTitle" placeholder="Judul notifikasi">
      </div>
      <div class="form-group">
        <label>Pesan</label>
        <input type="text" id="pushMessage" placeholder="Isi pesan">
      </div>
      <button class="btn btn-primary" style="width:100%;margin-top:15px;" onclick="sendPush()">Kirim</button>
    </div>
  </div>

  <script>
    let adminPass = '';

    function adminLogin() {
      adminPass = document.getElementById('adminPassword').value;
      if (!adminPass) return alert('Password wajib diisi');

      fetch('/api/admin/users?password=' + encodeURIComponent(adminPass))
        .then(r => r.json())
        .then(data => {
          if (data.success) {
            document.getElementById('loginForm').style.display = 'none';
            document.getElementById('mainContent').style.display = 'block';
            localStorage.setItem('admin_pass', adminPass);
            loadUsers();
            loadWaGroups();
          } else {
            alert('Password salah');
          }
        });
    }

    // Auto login if saved
    const savedPass = localStorage.getItem('admin_pass');
    if (savedPass) {
      document.getElementById('adminPassword').value = savedPass;
      adminLogin();
    }

    // ==================== WhatsApp Group Functions ====================
    function loadWaGroups() {
      const select = document.getElementById('waGroupSelect');
      select.innerHTML = '<option value="">Memuat grup...</option>';

      fetch('/api/admin/wa-groups?password=' + encodeURIComponent(adminPass))
        .then(r => r.json())
        .then(data => {
          if (!data.success) {
            select.innerHTML = '<option value="">Error: ' + (data.error || 'Unknown') + '</option>';
            return;
          }

          select.innerHTML = '<option value="">-- Pilih Grup (' + data.groups.length + ' grup) --</option>';
          data.groups.forEach(g => {
            const opt = document.createElement('option');
            opt.value = g.id;
            opt.textContent = g.name + ' (' + g.participants + ' member)' + (g.isMonitored ? ' [AKTIF]' : '');
            if (g.isMonitored) opt.selected = true;
            select.appendChild(opt);
          });

          // Show current group
          if (data.currentGroupId) {
            const current = data.groups.find(g => g.id === data.currentGroupId);
            if (current) {
              document.getElementById('currentGroup').innerHTML = 'Grup aktif: <strong style="color:#00ff88;">' + current.name + '</strong>';
            }
          } else {
            document.getElementById('currentGroup').textContent = 'Belum ada grup yang dipilih';
          }
        })
        .catch(e => {
          select.innerHTML = '<option value="">Error loading groups</option>';
        });
    }

    function setWaGroup() {
      const groupId = document.getElementById('waGroupSelect').value;
      const result = document.getElementById('syncResult');

      if (!groupId) {
        alert('Pilih grup terlebih dahulu');
        return;
      }

      fetch('/api/admin/wa-groups/set', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, groupId })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Grup berhasil di-set! Member baru yang masuk akan otomatis terdaftar.';
          loadWaGroups();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function syncMembers() {
      const result = document.getElementById('syncResult');
      result.className = 'result-msg success';
      result.textContent = 'Menyinkronkan member...';

      fetch('/api/admin/wa-groups/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Sync selesai! ' + data.added + ' user baru ditambahkan, ' + data.skipped + ' sudah ada. Total: ' + data.total + ' member.';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function clearInvalidUsers() {
      if (!confirm('Hapus semua user dengan nomor invalid (bukan format Indonesia 08xx)?')) return;

      const result = document.getElementById('syncResult');
      result.className = 'result-msg success';
      result.textContent = 'Menghapus user invalid...';

      fetch('/api/admin/users/clear-invalid', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Berhasil menghapus ' + data.deleted + ' user invalid.';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function clearAllUsers() {
      if (!confirm('⚠️ HAPUS SEMUA USER? Aksi ini tidak dapat dibatalkan!')) return;
      if (!confirm('Ketik OK untuk konfirmasi HAPUS SEMUA USER')) return;

      const result = document.getElementById('syncResult');
      result.className = 'result-msg success';
      result.textContent = 'Menghapus semua user...';

      fetch('/api/admin/users/clear-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, confirm: 'DELETE_ALL' })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Semua user berhasil dihapus.';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function loadUsers() {
      fetch('/api/admin/users?password=' + encodeURIComponent(adminPass))
        .then(r => r.json())
        .then(data => {
          if (!data.success) return;

          const users = data.users;
          const now = Date.now();

          let total = users.length;
          let active = users.filter(u => !u.expired || u.expired > now).length;
          let push = users.filter(u => u.hasPushSubscription).length;

          document.getElementById('totalUsers').textContent = total;
          document.getElementById('activeUsers').textContent = active;
          document.getElementById('pushUsers').textContent = push;

          const tbody = document.getElementById('userList');
          if (users.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">Belum ada user</td></tr>';
            return;
          }

          tbody.innerHTML = users.map(u => {
            let status, statusClass;
            if (!u.expired) {
              status = 'Lifetime';
              statusClass = 'status-lifetime';
            } else if (u.expired > now) {
              status = 'Aktif';
              statusClass = 'status-active';
            } else {
              status = 'Expired';
              statusClass = 'status-expired';
            }

            const expDate = u.expired ? new Date(u.expired).toLocaleDateString('id-ID') : '-';

            return '<tr>' +
              '<td>+62' + u.phone + '</td>' +
              '<td>' + (u.name || '-') + '</td>' +
              '<td><span class="status-badge ' + statusClass + '">' + status + '</span></td>' +
              '<td><span class="push-badge ' + (u.hasPushSubscription ? 'push-yes' : 'push-no') + '"></span></td>' +
              '<td>' + expDate + '</td>' +
              '<td>' +
                '<button class="btn btn-sm" onclick="editUser(\\'' + u.phone + '\\',\\'' + (u.name||'') + '\\')">Edit</button> ' +
                '<button class="btn btn-sm" onclick="openPushModal(\\'' + u.phone + '\\')">Push</button> ' +
                '<button class="btn btn-sm btn-danger" onclick="deleteUser(\\'' + u.phone + '\\')">Hapus</button>' +
              '</td>' +
            '</tr>';
          }).join('');
        });
    }

    function addUser() {
      const phone = document.getElementById('newPhone').value.trim();
      const name = document.getElementById('newName').value.trim();
      const expired = document.getElementById('newExpired').value;
      const result = document.getElementById('addResult');

      if (!phone) return alert('Nomor WA wajib diisi');

      fetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          password: adminPass,
          phone,
          name,
          expiredDays: expired ? parseInt(expired) : null
        })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'User berhasil ditambahkan!';
          document.getElementById('newPhone').value = '';
          document.getElementById('newName').value = '';
          document.getElementById('newExpired').value = '';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = data.error;
        }
        setTimeout(() => result.className = 'result-msg', 3000);
      });
    }

    function editUser(phone, name) {
      document.getElementById('editPhone').value = phone;
      document.getElementById('editName').value = name;
      document.getElementById('editAddDays').value = '';
      document.getElementById('editModal').classList.add('show');
    }

    function closeModal() {
      document.getElementById('editModal').classList.remove('show');
    }

    function saveUser() {
      const phone = document.getElementById('editPhone').value;
      const name = document.getElementById('editName').value;
      const addDays = document.getElementById('editAddDays').value;

      fetch('/api/admin/users', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          password: adminPass,
          phone,
          name,
          addDays: addDays ? parseInt(addDays) : null
        })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          closeModal();
          loadUsers();
        } else {
          alert(data.error);
        }
      });
    }

    function deleteUser(phone) {
      if (!confirm('Hapus user +62' + phone + '?')) return;

      fetch('/api/admin/users', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) loadUsers();
        else alert(data.error);
      });
    }

    function openPushModal(phone) {
      document.getElementById('pushPhone').value = phone || '';
      document.getElementById('pushTitle').value = '';
      document.getElementById('pushMessage').value = '';
      document.getElementById('pushModal').classList.add('show');
    }

    function closePushModal() {
      document.getElementById('pushModal').classList.remove('show');
    }

    function sendPush() {
      const phone = document.getElementById('pushPhone').value;
      const type = document.getElementById('pushType').value;
      const title = document.getElementById('pushTitle').value;
      const message = document.getElementById('pushMessage').value;

      if (!title || !message) return alert('Judul dan pesan wajib diisi');

      fetch('/api/admin/push', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone: phone || null, type, title, message })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          alert('Notifikasi terkirim ke ' + data.sent + ' user');
          closePushModal();
        } else {
          alert(data.error);
        }
      });
    }
  </script>
</body>
</html>`;
  res.send(html);
})

// MONITORING PAGE - Professional Gold Price Dashboard
app.get('/monitoring', async (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  res.setHeader('Pragma', 'no-cache')
  res.setHeader('Pragma', 'no-cache')
  res.setHeader('Expires', '0')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="theme-color" content="#f7931a">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <link rel="manifest" href="/manifest.json">
  <link rel="apple-touch-icon" href="/icon.png">
  <link rel="icon" type="image/x-icon" href="/favicon.ico">
  <link rel="icon" type="image/png" href="/icon.png">
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

    /* Notification Banner */
    #notifContainer {
      display: flex;
      flex-direction: column;
      gap: 10px;
      margin-bottom: 15px;
    }
    .notif-banner {
      background: linear-gradient(135deg, #1a1f26 0%, #2f3640 100%);
      border-radius: 10px;
      padding: 12px 15px;
      display: flex;
      align-items: center;
      gap: 12px;
      border-left: 4px solid #3498db;
      animation: slideDown 0.3s ease;
    }
    .notif-banner.promo { border-left-color: #f7931a; background: linear-gradient(135deg, #2a2010 0%, #3a3020 100%); }
    .notif-banner.warning { border-left-color: #f39c12; background: linear-gradient(135deg, #2a2510 0%, #3a3520 100%); }
    .notif-banner.urgent { border-left-color: #e74c3c; background: linear-gradient(135deg, #2a1515 0%, #3a2020 100%); }
    .notif-banner.info { border-left-color: #3498db; background: linear-gradient(135deg, #152025 0%, #203035 100%); }
    .notif-icon {
      width: 36px;
      height: 36px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      font-size: 18px;
    }
    .notif-banner.promo .notif-icon { background: #f7931a; }
    .notif-banner.warning .notif-icon { background: #f39c12; }
    .notif-banner.urgent .notif-icon { background: #e74c3c; }
    .notif-banner.info .notif-icon { background: #3498db; }
    .notif-content {
      flex: 1;
      min-width: 0;
    }
    .notif-title {
      font-size: 14px;
      font-weight: bold;
      color: #fff;
      margin-bottom: 2px;
    }
    .notif-message {
      font-size: 13px;
      color: #b0b0b0;
      line-height: 1.3;
    }
    .notif-close {
      background: rgba(255,255,255,0.1);
      border: none;
      color: #888;
      font-size: 18px;
      cursor: pointer;
      padding: 5px 10px;
      border-radius: 5px;
      transition: all 0.2s;
    }
    .notif-close:hover { background: rgba(255,255,255,0.2); color: #fff; }
    @keyframes slideDown {
      from { transform: translateY(-20px); opacity: 0; }
      to { transform: translateY(0); opacity: 1; }
    }

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
        <button onclick="logout()" style="margin-top:5px;padding:4px 10px;background:#ff4444;border:none;border-radius:5px;color:white;font-size:0.7em;cursor:pointer;">Logout</button>
      </div>
    </div>

    <!-- Notification Banner Container -->
    <div id="notifContainer"></div>

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

    // Sound Notification - berbeda untuk naik dan turun menggunakan Web Audio API
    let soundEnabled = localStorage.getItem('soundEnabled') !== 'false';
    let audioContext = null;

    function getAudioContext() {
      if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
      }
      return audioContext;
    }

    function toggleSound() {
      soundEnabled = !soundEnabled;
      localStorage.setItem('soundEnabled', soundEnabled);
      document.getElementById('soundStatus').textContent = soundEnabled ? 'ON' : 'OFF';
      document.getElementById('soundToggle').style.opacity = soundEnabled ? '1' : '0.5';
    }

    function playSound(direction) {
      if (!soundEnabled) return;

      try {
        const ctx = getAudioContext();
        if (ctx.state === 'suspended') ctx.resume();

        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        if (direction === 'up') {
          // JP JP sound - 2 beep naik (cheerful)
          oscillator.type = 'sine';
          oscillator.frequency.setValueAtTime(800, ctx.currentTime);
          oscillator.frequency.setValueAtTime(1000, ctx.currentTime + 0.1);
          oscillator.frequency.setValueAtTime(800, ctx.currentTime + 0.2);
          oscillator.frequency.setValueAtTime(1200, ctx.currentTime + 0.3);
          gainNode.gain.setValueAtTime(0.3, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.5);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.5);
        } else {
          // SORRR sound - slide down (sad)
          oscillator.type = 'sawtooth';
          oscillator.frequency.setValueAtTime(400, ctx.currentTime);
          oscillator.frequency.exponentialRampToValueAtTime(100, ctx.currentTime + 0.5);
          gainNode.gain.setValueAtTime(0.2, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.5);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.5);
        }
      } catch (e) {
        console.log('Audio error:', e);
      }
    }

    // Update sound status on load
    document.getElementById('soundStatus').textContent = soundEnabled ? 'ON' : 'OFF';
    document.getElementById('soundToggle').style.opacity = soundEnabled ? '1' : '0.5';

    // Browser Notification
    let notifEnabled = false;

    async function requestNotificationPermission() {
      if (!('Notification' in window)) {
        alert('Browser tidak mendukung notifikasi');
        return false;
      }

      if (Notification.permission === 'granted') {
        notifEnabled = true;
        subscribeToPush(); // Subscribe to push when permission already granted
        return true;
      }

      if (Notification.permission !== 'denied') {
        const permission = await Notification.requestPermission();
        if (permission === 'granted') {
          notifEnabled = true;
          subscribeToPush(); // Subscribe to push after permission granted
          return true;
        }
      }

      return false;
    }

    // Promo/Info Notification Banner
    function showPromoNotification(data) {
      console.log('showPromoNotification called:', data);
      const container = document.getElementById('notifContainer');
      console.log('notifContainer element:', container);
      if (!container) {
        console.error('notifContainer not found!');
        return;
      }

      // Icon berdasarkan type
      const icons = {
        promo: '\u{1F381}',
        warning: '\u26A0\uFE0F',
        urgent: '\u{1F6A8}',
        info: '\u{1F4E2}'
      };

      const banner = document.createElement('div');
      banner.className = 'notif-banner ' + (data.notifType || 'info');
      banner.innerHTML = \`
        <div class="notif-icon">\${icons[data.notifType] || icons.info}</div>
        <div class="notif-content">
          <div class="notif-title">\${data.title}</div>
          <div class="notif-message">\${data.message}</div>
        </div>
        <button class="notif-close" onclick="this.parentElement.remove()">&times;</button>
      \`;

      container.insertBefore(banner, container.firstChild);

      // Browser notification juga
      if (notifEnabled && Notification.permission === 'granted') {
        new Notification(data.title, {
          body: data.message,
          icon: '/icon.png',
          tag: 'promo-' + Date.now()
        });
      }

      // Play sound
      playSound('up');
    }

    // Fungsi untuk tutup popup promo
    window.closePromoPopup = function(el) {
      el.parentElement.parentElement.remove();
    };

    function showNotification(title, body, isUp) {
      if (!notifEnabled || Notification.permission !== 'granted') return;

      const options = {
        body: body,
        icon: '/icon.png',
        badge: '/icon.png',
        tag: 'gold-price',
        renotify: true,
        silent: false
      };

      try {
        new Notification(title, options);
      } catch (e) {
        console.log('Notification error:', e);
      }
    }

    // Minta izin notifikasi saat halaman load
    if ('Notification' in window && Notification.permission === 'granted') {
      notifEnabled = true;
    } else if ('Notification' in window && Notification.permission !== 'denied') {
      // Tampilkan prompt untuk minta izin
      setTimeout(() => {
        requestNotificationPermission();
      }, 3000);
    }

    // ==================== AUTH CHECK ====================
    (async function checkAuth() {
      const session = localStorage.getItem('goldmonitor_session');
      if (!session) {
        window.location.href = '/login';
        return;
      }

      try {
        const res = await fetch('/api/verify-session?session=' + session);
        const data = await res.json();
        if (!data.valid) {
          localStorage.removeItem('goldmonitor_session');
          localStorage.removeItem('goldmonitor_user');
          window.location.href = '/login';
          return;
        }
        // Display user name if available
        if (data.user && data.user.name) {
          const header = document.querySelector('.header-left .subtitle');
          if (header) header.textContent = 'Welcome, ' + data.user.name;
        }
      } catch (e) {
        // Allow offline access
        console.log('Auth check failed, allowing offline access');
      }
    })();

    // Logout function
    window.logout = async function() {
      const session = localStorage.getItem('goldmonitor_session');
      if (session) {
        await fetch('/api/logout', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ session })
        });
      }
      localStorage.removeItem('goldmonitor_session');
      localStorage.removeItem('goldmonitor_user');
      window.location.href = '/login';
    };

    // ==================== PUSH NOTIFICATION SUBSCRIPTION ====================
    async function subscribeToPush() {
      if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
        console.log('Push not supported');
        return;
      }

      try {
        const registration = await navigator.serviceWorker.ready;

        // Get VAPID public key
        const vapidRes = await fetch('/api/vapid-public-key');
        const { publicKey } = await vapidRes.json();

        // Convert VAPID key
        const applicationServerKey = urlBase64ToUint8Array(publicKey);

        // Subscribe
        const subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey
        });

        // Send to server
        const session = localStorage.getItem('goldmonitor_session');
        if (session) {
          await fetch('/api/push-subscribe', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session, subscription })
          });
          console.log('Push subscription saved');
        }
      } catch (e) {
        console.log('Push subscription failed:', e);
      }
    }

    function urlBase64ToUint8Array(base64String) {
      const padding = '='.repeat((4 - base64String.length % 4) % 4);
      const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
      const rawData = window.atob(base64);
      const outputArray = new Uint8Array(rawData.length);
      for (let i = 0; i < rawData.length; ++i) {
        outputArray[i] = rawData.charCodeAt(i);
      }
      return outputArray;
    }

    // Register Service Worker for PWA
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js')
        .then(() => {
          // Subscribe to push after SW registered
          if (Notification.permission === 'granted') {
            subscribeToPush();
          }
        })
        .catch(() => {});
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

        // Log semua data yang masuk (kecuali heartbeat)
        if (data.type !== 'heartbeat') {
          console.log('SSE data received:', data.type, data);
        }

        if (data.type === 'heartbeat') {
          console.log('\u{1F493} Heartbeat received');
          return;
        }

        // Handle notifikasi/promo dari admin
        if (data.type === 'notification') {
          console.log('\u{1F514} Notification received:', data);
          showPromoNotification(data);
          return;
        }

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
              playSound(change > 0 ? 'up' : 'down');

              // Browser Notification
              const notifTitle = change > 0 ? 'Harga Emas NAIK' : 'Harga Emas TURUN';
              const notifBody = 'Rp ' + data.buy.toLocaleString('id-ID') + ' (' + sign + change.toLocaleString('id-ID') + ')';
              showNotification(notifTitle, notifBody, change > 0);

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
  await loadMonitoredGroup()

  // Clear any corrupt Redis auth from previous attempts
  await clearRedisAuth()

  // Use file-based auth (more stable)
  const { state, saveCreds } = await useMultiFileAuthState('./auth')
  const { version } = await fetchLatestBaileysVersion()

  pushLog('WA | Using file-based auth state')

  sock = makeWASocket({
    version,
    logger: pino({ level: 'silent' }),
    printQRInTerminal: true,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
    },
    browser: Browsers.ubuntu('Chrome'),
    markOnlineOnConnect: false,
    syncFullHistory: false,
    defaultQueryTimeoutMs: 120000,
    keepAliveIntervalMs: 25000,
    connectTimeoutMs: 60000,
    qrTimeout: 60000,
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

  // ==================== GROUP PARTICIPANT UPDATE ====================
  sock.ev.on('group-participants.update', async (update) => {
    try {
      const { id, participants, action } = update

      // Hanya proses jika ini grup yang di-monitor
      if (!monitoredGroupId || id !== monitoredGroupId) return

      for (const participant of participants) {
        const phone = extractPhoneFromJid(participant)
        if (!phone) continue

        if (action === 'add') {
          // Member baru masuk grup
          await autoRegisterGroupMember(phone)
        } else if (action === 'remove') {
          // Member keluar/dikeluarkan dari grup
          await removeGroupMember(phone)
        }
      }
    } catch (e) {
      pushLog('WA | Group update error: ' + e.message)
    }
  })

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
