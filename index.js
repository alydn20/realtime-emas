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
import crypto from 'crypto'

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

// Admin phones for notifications (dapat diubah via menu admin)
let ADMIN_PHONES = ['62895701692525'] // Default admin phone

// Pending registrations now stored in Redis (REDIS_KEYS.PENDING_REGISTRATIONS)

const REDIS_KEYS = {
  DAILY_STATS: 'gold:daily_stats',
  PRICE_HISTORY: 'gold:price_history',
  USERS: 'gold:users',           // Hash: phone -> user data (name, expired, createdAt, pin, pinChanged)
  PUSH_SUBS: 'gold:push_subs',   // Hash: phone -> push subscription JSON
  SESSIONS: 'gold:sessions',     // Hash: sessionId -> phone
  WA_GROUP_ID: 'gold:wa_group_id', // String: ID grup WA yang di-monitor
  WA_AUTH: 'gold:wa_auth',       // Hash: key -> auth data (creds, keys) for persistent WA session
  OTP_CODES: 'gold:otp_codes',   // Hash: phone -> OTP code for registration verification
  LOGIN_TOKENS: 'gold:login_tokens', // Hash: token -> { phone, expires }
  LOGIN_ATTEMPTS: 'gold:login_attempts', // Hash: phone -> { attempts, lastAttempt }
  BLOCKED_USERS: 'gold:blocked_users', // Hash: phone -> { blockedAt, reason }
  PENDING_REGISTRATIONS: 'gold:pending_reg_v2', // Hash: phone -> { name, phone, timestamp }
  USER_PINS: 'gold:user_pins',    // Hash: phone -> { pin (hashed), pinChanged (boolean) }
  SOUND_SETTINGS: 'gold:sound_settings' // JSON: custom sound settings (soundUp, soundDown URLs)
}

// Admin password untuk akses admin panel
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin123'

// Super Admin credentials untuk akses /qr dan /admin
const SUPER_ADMIN = {
  username: 'admin',
  password: 'admin'
}

// ID Grup WhatsApp yang membernya otomatis terdaftar (di-set via admin panel)
let monitoredGroupId = null

// ==================== REDIS AUTH STATE (Persistent WA Session) ====================
async function useRedisAuthState() {
  const writeData = async (key, data) => {
    try {
      const serialized = JSON.stringify(data, BufferJSON.replacer)
      await redis.hset(REDIS_KEYS.WA_AUTH, { [key]: serialized })
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
  } else {
    pushLog('WA | Loaded existing credentials from Redis')
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
      pushLog('WA | Credentials saved to Redis')
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

    await redis.hset(REDIS_KEYS.USERS, { [phone]: JSON.stringify(userData) })
    pushLog('WA | Auto-registered: +62' + phone)
  } catch (e) {
    pushLog('WA | Auto-register failed: ' + e.message)
  }
}

// Remove member dari database saat keluar/kick dari grup
async function removeGroupMember(phone) {
  if (!phone) return

  try {
    const existing = await redis.hget(REDIS_KEYS.USERS, phone)
    if (!existing) return

    // Hapus user apapun source-nya (baik dari whatsapp_group, manual, OTP, dll)
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

    pushLog('WA | Auto-removed member (kicked/left): +62' + phone)
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

// Update daily stats - DISABLED (not needed)
function updateDailyStats(buyPrice) {
  // Daily stats disabled - tidak digunakan
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

// Add price history ke LOCAL memory only (no Redis)
let lastAddedUpdatedAt = '' // Track updatedAt terakhir yang sudah ditambahkan
const addedTimestamps = new Set() // Track semua timestamp yang sudah ditambahkan

function addPriceHistory(buy, sell, prevBuy, prevSell, updatedAt) {
  // Skip jika updatedAt kosong atau sama dengan yang terakhir
  if (!updatedAt || updatedAt === lastAddedUpdatedAt) return

  // Cek apakah timestamp sudah pernah ditambahkan (anti-duplikat)
  if (addedTimestamps.has(updatedAt)) return

  // Cek dari cache lokal
  const existsInCache = priceHistoryCache.some(entry => entry.time === updatedAt)
  if (existsInCache) {
    addedTimestamps.add(updatedAt)
    lastAddedUpdatedAt = updatedAt
    return
  }

  // Calculate spread percentage
  const spread = ((sell - buy) / buy * 100).toFixed(2)

  const entry = {
    time: updatedAt,
    buy: buy,
    sell: sell,
    buyChange: buy - prevBuy,
    sellChange: sell - prevSell,
    spread: spread,
    usdIdr: cachedMarketData.usdIdr?.rate || 0
  }

  // Simpan ke local cache
  priceHistoryCache.push(entry)
  addedTimestamps.add(updatedAt)
  lastAddedUpdatedAt = updatedAt

  // Limit max 1440 entries (24 jam)
  if (priceHistoryCache.length > 1440) {
    priceHistoryCache.shift()
  }

  // Limit addedTimestamps
  if (addedTimestamps.size > 200) {
    const arr = Array.from(addedTimestamps)
    addedTimestamps.clear()
    arr.slice(-100).forEach(t => addedTimestamps.add(t))
  }
}

// Get price history dengan pagination (local memory)
function getPriceHistory(page = 1, perPage = 10) {
  const total = priceHistoryCache.length
  const totalPages = Math.ceil(total / perPage)

  // Ambil dari akhir (terbaru) dengan pagination
  const start = Math.max(0, total - (page * perPage))
  const end = total

  const items = priceHistoryCache.slice(start, end).reverse()

  return {
    items: items,
    page: page,
    perPage: perPage,
    total: total,
    totalPages: totalPages
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
📊 Lihat Chart & Riwayat Lengkap:
🔗 https://ts.muhamadaliyudin.xyz`
}
async function fetchTreasury() {
  const res = await fetch(TREASURY_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Connection': 'keep-alive'
    },
    agent: httpsAgent, // Reuse TCP connection
    signal: AbortSignal.timeout(5000) // 5 detik timeout (lebih toleran untuk network latency)
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
// Polling terus-menerus untuk real-time update
let isFastPolling = false
let lastKnownTimestamp = 0
let consecutiveErrors = 0

async function fastPoll() {
  if (isFastPolling) return
  isFastPolling = true

  try {
    const treasuryData = await fetchTreasury()

    if (!treasuryData?.data?.buying_rate) {
      consecutiveErrors++
      return
    }

    consecutiveErrors = 0
    lastSuccessfulFetch = Date.now() // Track successful fetch

    const currentPrice = {
      buy: treasuryData.data.buying_rate,
      sell: treasuryData.data.selling_rate,
      updated_at: treasuryData.data.updated_at,
      fetchedAt: Date.now()
    }

    const updateTime = new Date(treasuryData.data.updated_at).getTime()
    const isNewTimestamp = updateTime > lastKnownTimestamp
    const isPriceChanged = lastKnownPrice &&
      (lastKnownPrice.buy !== currentPrice.buy || lastKnownPrice.sell !== currentPrice.sell)

    if (isNewTimestamp) {
      lastKnownTimestamp = updateTime
      lastApiUpdateTime = treasuryData.data.updated_at
    }

    const prevPrice = lastKnownPrice ? { ...lastKnownPrice } : null

    if (!lastKnownPrice) {
      lastKnownPrice = currentPrice
      await updateDailyStats(currentPrice.buy)
      broadcastSSE({
        type: 'price',
        buy: currentPrice.buy,
        sell: currentPrice.sell,
        updatedAt: currentPrice.updated_at,
        usdIdr: cachedMarketData.usdIdr?.rate,
        xauUsd: cachedMarketData.xauUsd,
        serverTime: new Date().toISOString()
      })
    } else if (isPriceChanged) {
      lastKnownPrice = currentPrice
      if (currentPrice.updated_at !== lastAddedUpdatedAt) {
        await updateDailyStats(currentPrice.buy)
        await addPriceHistory(currentPrice.buy, currentPrice.sell, prevPrice.buy, prevPrice.sell, currentPrice.updated_at)
      }
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
    } else {
      lastKnownPrice = currentPrice
    }
  } catch (e) {
    consecutiveErrors++
    // Log setiap error untuk debugging
    if (consecutiveErrors === 1 || consecutiveErrors % 100 === 0) {
      pushLog('TREASURY | Fetch error #' + consecutiveErrors + ': ' + e.message)
    }
  } finally {
    isFastPolling = false
  }
}

// Fast poll setiap 500ms (balanced - 2x per detik)
setInterval(fastPoll, 500)
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

// ==================== PERIODIC PRICE BROADCAST ====================
// Kirim update harga setiap 10 detik meskipun harga tidak berubah
// Ini memastikan client selalu mendapat data terbaru dan timestamp update
let lastPeriodicBroadcast = 0
let lastSuccessfulFetch = Date.now() // Track kapan terakhir fetch berhasil

setInterval(() => {
  if (lastKnownPrice && sseClients.size > 0) {
    const now = Date.now()
    // Broadcast setiap 10 detik
    if (now - lastPeriodicBroadcast >= 10000) {
      lastPeriodicBroadcast = now
      broadcastSSE({
        type: 'price',
        buy: lastKnownPrice.buy,
        sell: lastKnownPrice.sell,
        updatedAt: lastKnownPrice.updated_at,
        usdIdr: cachedMarketData.usdIdr?.rate,
        xauUsd: cachedMarketData.xauUsd,
        serverTime: new Date().toISOString()
      })
    }
  }

  // Log warning jika tidak ada successful fetch dalam 30 detik
  const now = Date.now()
  if (now - lastSuccessfulFetch > 30000) {
    pushLog('TREASURY | Warning: No successful fetch in 30+ seconds! Consecutive errors: ' + consecutiveErrors)
    lastSuccessfulFetch = now // Reset untuk hindari spam log
  }
}, 2000) // Check setiap 2 detik

// ==================== STARTUP INFO ====================
console.log(`[GOLD] Bot started | Price check: ${PRICE_CHECK_INTERVAL/1000}s | Stale alert: ${STALE_PRICE_THRESHOLD/60000}min`)

const app = express()
app.use(express.json())

// ==================== SUPER ADMIN LOGIN ====================
// Login page untuk akses /qr dan /admin
app.get('/admin-login', (req, res) => {
  const { redirect } = req.query
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <title>Admin Login - Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: linear-gradient(145deg, #0a0e13 0%, #131921 50%, #0f1419 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      position: relative;
      overflow: hidden;
    }
    body::before {
      content: '';
      position: absolute;
      top: -50%;
      left: -50%;
      width: 200%;
      height: 200%;
      background: radial-gradient(circle at 30% 20%, rgba(220,38,38,0.08) 0%, transparent 50%),
                  radial-gradient(circle at 70% 80%, rgba(220,38,38,0.05) 0%, transparent 40%);
      animation: float 20s ease-in-out infinite;
      pointer-events: none;
    }
    @keyframes float {
      0%, 100% { transform: translate(0, 0) rotate(0deg); }
      50% { transform: translate(-2%, 2%) rotate(1deg); }
    }
    .card {
      background: rgba(20, 26, 34, 0.9);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-radius: 24px;
      padding: 40px 32px;
      width: 100%;
      max-width: 400px;
      border: 1px solid rgba(255,255,255,0.08);
      box-shadow: 0 25px 80px rgba(0,0,0,0.5),
                  0 0 0 1px rgba(255,255,255,0.05) inset;
      position: relative;
      z-index: 1;
    }
    .admin-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: linear-gradient(135deg, rgba(220,38,38,0.2), rgba(220,38,38,0.1));
      color: #f87171;
      padding: 8px 14px;
      border-radius: 20px;
      font-size: 0.75em;
      font-weight: 600;
      margin-bottom: 20px;
      border: 1px solid rgba(220,38,38,0.2);
    }
    .admin-badge svg { width: 14px; height: 14px; }
    h1 {
      color: #ffffff;
      text-align: center;
      margin-bottom: 8px;
      font-size: 1.6em;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .subtitle {
      color: #8b949e;
      text-align: center;
      margin-bottom: 32px;
      font-size: 0.9em;
      font-weight: 400;
    }
    .form-group {
      margin-bottom: 20px;
    }
    label {
      display: block;
      color: #8b949e;
      margin-bottom: 10px;
      font-size: 0.85em;
      font-weight: 500;
    }
    input {
      width: 100%;
      padding: 16px 18px;
      border: 2px solid rgba(255,255,255,0.08);
      border-radius: 14px;
      background: rgba(15, 20, 25, 0.8);
      color: #e7e9ea;
      font-size: 1em;
      font-family: inherit;
      transition: all 0.2s ease;
    }
    input:focus {
      outline: none;
      border-color: #dc2626;
      background: rgba(15, 20, 25, 1);
      box-shadow: 0 0 0 4px rgba(220,38,38,0.15);
    }
    input::placeholder { color: #4a5568; }
    .btn {
      width: 100%;
      padding: 16px;
      background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%);
      color: white;
      border: none;
      border-radius: 14px;
      font-size: 1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      font-family: inherit;
      box-shadow: 0 4px 20px rgba(220,38,38,0.35);
    }
    .btn:hover {
      transform: translateY(-2px);
      box-shadow: 0 8px 30px rgba(220,38,38,0.45);
    }
    .btn:active { transform: translateY(0); }
    .error {
      background: rgba(239,68,68,0.12);
      border: 1px solid rgba(239,68,68,0.3);
      color: #f87171;
      padding: 14px 16px;
      border-radius: 12px;
      margin-bottom: 20px;
      text-align: left;
      display: none;
      font-size: 0.9em;
      font-weight: 500;
    }
    .error.show { display: block; }
    .back-link {
      display: block;
      text-align: center;
      margin-top: 24px;
      color: #8b949e;
      font-size: 0.85em;
      text-decoration: none;
    }
    .back-link:hover { color: #f7931a; }
    @media (max-width: 480px) {
      .card { padding: 32px 24px; border-radius: 20px; }
      h1 { font-size: 1.4em; }
      input { padding: 14px 16px; }
      .btn { padding: 14px; }
    }
  </style>
</head>
<body>
  <div class="card">
    <div style="text-align:center;">
      <span class="admin-badge">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
        Admin Area
      </span>
    </div>
    <h1>Admin Login</h1>
    <p class="subtitle">Masuk untuk mengakses panel admin</p>
    <div class="error" id="error">Username atau password salah</div>
    <form id="loginForm">
      <div class="form-group">
        <label>Username</label>
        <input type="text" id="username" placeholder="Masukkan username" required>
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" id="password" placeholder="Masukkan password" required>
      </div>
      <button type="submit" class="btn">Login Admin</button>
    </form>
    <a href="/login" class="back-link">← Kembali ke halaman user</a>
  </div>
  <script>
    document.getElementById('loginForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const username = document.getElementById('username').value;
      const password = document.getElementById('password').value;
      const error = document.getElementById('error');

      try {
        const res = await fetch('/api/admin-login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, password })
        });
        const data = await res.json();

        if (data.success) {
          localStorage.setItem('super_admin_token', data.token);
          // Save monitoring session so admin can access /monitoring
          if (data.monitoringSession) {
            localStorage.setItem('goldmonitor_session', data.monitoringSession);
          }
          window.location.href = '${redirect || '/admin/users'}';
        } else {
          error.classList.add('show');
        }
      } catch (err) {
        error.textContent = 'Terjadi kesalahan';
        error.classList.add('show');
      }
    });
  </script>
</body>
</html>`)
})

// API untuk login
app.post('/api/admin-login', async (req, res) => {
  const { username, password } = req.body
  if (username === SUPER_ADMIN.username && password === SUPER_ADMIN.password) {
    // Generate simple token
    const token = Buffer.from(username + ':' + password + ':' + Date.now()).toString('base64')

    // Create admin session for monitoring access
    const adminSessionId = 'admin_' + crypto.randomBytes(16).toString('hex')
    await redis.hset(REDIS_KEYS.SESSIONS, { [adminSessionId]: 'admin' })

    // Also add admin to users hash if not exists (for session validation)
    const adminUserData = JSON.stringify({ name: 'Administrator', phone: 'admin', isAdmin: true })
    await redis.hset(REDIS_KEYS.USERS, { 'admin': adminUserData })

    res.json({ success: true, token, monitoringSession: adminSessionId })
  } else {
    res.json({ success: false, error: 'Invalid credentials' })
  }
})

// API untuk verify token
app.post('/api/verify-admin', (req, res) => {
  const { token } = req.body
  try {
    const decoded = Buffer.from(token, 'base64').toString()
    const [username, password] = decoded.split(':')
    if (username === SUPER_ADMIN.username && password === SUPER_ADMIN.password) {
      res.json({ success: true })
    } else {
      res.json({ success: false })
    }
  } catch (e) {
    res.json({ success: false })
  }
})

// Helper function untuk generate auth check script
function getAuthCheckScript(redirectTo) {
  return `
  <script>
    (async function() {
      const token = localStorage.getItem('super_admin_token');
      if (!token) {
        window.location.href = '/admin-login?redirect=${encodeURIComponent(redirectTo)}';
        return;
      }

      try {
        const res = await fetch('/api/verify-admin', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token })
        });
        const data = await res.json();
        if (!data.success) {
          localStorage.removeItem('super_admin_token');
          window.location.href = '/admin-login?redirect=${encodeURIComponent(redirectTo)}';
        }
      } catch (e) {
        window.location.href = '/admin-login?redirect=${encodeURIComponent(redirectTo)}';
      }
    })();
  </script>`
}

app.get('/', (_req, res) => {
  res.redirect('/login')
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
  // Auth check akan di-inject di halaman
  const authScript = getAuthCheckScript('/qr')
  if (!lastQr) {
    const statusMsg = isReady
      ? '<span style="color:#00ff88;">✓ WhatsApp sudah terhubung!</span><br><small style="color:#71767b;">Bot aktif dan siap digunakan.</small>'
      : '<span style="color:#ffaa00;">⏳ Menunggu QR Code...</span><br><small style="color:#71767b;">Jika tidak muncul dalam 30 detik, coba Reset.</small>'

    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>WhatsApp Status</title></head><body>
    ${authScript}
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
  </body></html>`)
  }

  try {
    const mod = await import('qrcode').catch(() => null)
    if (mod?.toDataURL) {
      const dataUrl = await mod.toDataURL(lastQr, { margin: 1 })
      return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Scan QR WhatsApp</title></head><body>
        ${authScript}
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
      </body></html>`)
    }
  } catch (_) {}
  res.send(lastQr)
})

// Reset QR - Hapus session dan restart koneksi WA
app.get('/qr-reset', async (req, res) => {
  const { confirm } = req.query
  const authScript = getAuthCheckScript('/qr-reset')

  if (confirm !== 'yes') {
    return res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Reset WhatsApp</title></head><body>
      ${authScript}
      <div style="text-align:center;padding:40px;font-family:sans-serif;background:#0f1419;color:#e7e9ea;min-height:100vh;">
        <h2 style="color:#ff4444;">Reset WhatsApp Session</h2>
        <p style="margin:20px 0;color:#71767b;">Ini akan menghapus sesi WhatsApp dan memerlukan scan QR ulang.</p>
        <p style="margin:20px 0;color:#ffaa00;">⚠️ WhatsApp akan logout dari device ini!</p>
        <a href="/qr-reset?confirm=yes" style="display:inline-block;margin:10px;padding:15px 30px;background:#ff4444;color:white;text-decoration:none;border-radius:10px;font-weight:bold;">Ya, Reset Sekarang</a>
        <a href="/qr" style="display:inline-block;margin:10px;padding:15px 30px;background:#2f3640;color:white;text-decoration:none;border-radius:10px;">Batal</a>
      </div>
    </body></html>`)
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

    // Delete local auth folder
    const fs = await import('fs')
    const path = await import('path')
    const authPath = path.join(process.cwd(), 'auth')

    if (fs.existsSync(authPath)) {
      fs.rmSync(authPath, { recursive: true, force: true })
      pushLog('WA | Auth folder deleted')
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
  // Include current USD/IDR for fallback on old entries
  history.currentUsdIdr = cachedMarketData.usdIdr?.rate || 0
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

// Remove duplicate entries from history
app.get('/cleanup-history', async (req, res) => {
  try {
    const allHistory = await redis.lrange(REDIS_KEYS.PRICE_HISTORY, 0, -1)
    const seen = new Set()
    const uniqueHistory = []

    for (const entry of allHistory) {
      const parsed = typeof entry === 'string' ? JSON.parse(entry) : entry
      if (!seen.has(parsed.time)) {
        seen.add(parsed.time)
        uniqueHistory.push(entry)
      }
    }

    const removed = allHistory.length - uniqueHistory.length

    if (removed > 0) {
      await redis.del(REDIS_KEYS.PRICE_HISTORY)
      for (const entry of uniqueHistory) {
        await redis.rpush(REDIS_KEYS.PRICE_HISTORY, entry)
      }
      priceHistoryCache = uniqueHistory.map(e => typeof e === 'string' ? JSON.parse(e) : e)
      addedTimestamps.clear()
      uniqueHistory.forEach(e => {
        const parsed = typeof e === 'string' ? JSON.parse(e) : e
        addedTimestamps.add(parsed.time)
      })
    }

    res.json({ success: true, removed: removed, remaining: uniqueHistory.length })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// SSE (Server-Sent Events) untuk real-time push ke frontend
// Map: res -> { phone, name, connectedAt, lastActivity }
const sseClients = new Map()

app.get('/sse', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.flushHeaders()

  // Get user info from session
  const session = req.query.session || ''
  let userInfo = { phone: 'anonymous', name: 'Anonymous', connectedAt: new Date().toISOString(), lastActivity: Date.now() }

  if (session) {
    try {
      const phone = await redis.hget(REDIS_KEYS.SESSIONS, session)
      if (phone) {
        userInfo.phone = phone
        const userData = await redis.hget(REDIS_KEYS.USERS, phone)
        if (userData) {
          const parsed = JSON.parse(userData)
          userInfo.name = parsed.name || ('Member ' + phone)
        } else {
          // Fallback untuk admin atau user tanpa data lengkap
          userInfo.name = phone === 'admin' ? 'Administrator' : ('Member ' + phone)
        }
      }
    } catch (e) {}
  }

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

  sseClients.set(res, userInfo)

  // Broadcast online users update to admin
  broadcastOnlineUsers()

  req.on('close', () => {
    sseClients.delete(res)
    // Broadcast online users update when someone disconnects
    broadcastOnlineUsers()
  })
})

// Fungsi untuk broadcast ke semua SSE clients
function broadcastSSE(data) {
  const message = `data: ${JSON.stringify(data)}\n\n`
  console.log(`[BROADCAST] Type: ${data.type}, Clients: ${sseClients.size}`)
  sseClients.forEach((userInfo, client) => {
    try {
      client.write(message)
    } catch (e) {
      sseClients.delete(client)
    }
  })
}

// Fungsi untuk get online users list
function getOnlineUsers() {
  const users = []
  const seen = new Set()
  sseClients.forEach((userInfo, client) => {
    // Avoid duplicates by phone
    if (!seen.has(userInfo.phone)) {
      seen.add(userInfo.phone)
      users.push({
        phone: userInfo.phone,
        name: userInfo.name,
        connectedAt: userInfo.connectedAt
      })
    }
  })
  return users
}

// Broadcast online users ke admin SSE (separate channel)
const adminSseClients = new Set()

function broadcastOnlineUsers() {
  const users = getOnlineUsers()
  const message = `data: ${JSON.stringify({ type: 'online_users', users, count: sseClients.size })}\n\n`
  adminSseClients.forEach(client => {
    try {
      client.write(message)
    } catch (e) {
      adminSseClients.delete(client)
    }
  })
}

// SSE endpoint untuk admin (online users monitoring)
app.get('/admin-sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.flushHeaders()

  // Send initial online users data
  const users = getOnlineUsers()
  res.write(`data: ${JSON.stringify({ type: 'online_users', users, count: sseClients.size })}\n\n`)

  adminSseClients.add(res)

  req.on('close', () => {
    adminSseClients.delete(res)
  })
})

// API untuk get online users (non-realtime)
app.get('/api/admin/online-users', (req, res) => {
  const { password } = req.query
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  const users = getOnlineUsers()
  res.json({
    success: true,
    count: sseClients.size,
    uniqueUsers: users.length,
    users
  })
})

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

// SSE Heartbeat - kirim ping setiap 5 detik untuk menjaga koneksi aktif dan responsif
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
}, 5000)

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
app.get('/manifest.json', (req, res) => {
  const host = req.get('host') || 'ts.muhamadaliyudin.xyz'
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
    ],
    related_applications: [
      {
        platform: 'webapp',
        url: 'https://' + host + '/manifest.json'
      }
    ],
    prefer_related_applications: false
  })
})

// Service Worker for PWA - v4 dengan Push Notifications
app.get('/sw.js', (_req, res) => {
  res.setHeader('Content-Type', 'application/javascript')
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  res.send(`
    const CACHE_VERSION = 'gold-monitor-v5';

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
  const authScript = getAuthCheckScript('/admin/monitoring')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <title>Admin - Gold Price Monitor</title>
${authScript}
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: linear-gradient(180deg, #0a0e13 0%, #0f1419 100%);
      min-height: 100vh;
      padding: 20px;
      color: #e7e9ea;
    }
    .container { max-width: 640px; margin: 0 auto; }

    .header {
      text-align: center;
      margin-bottom: 24px;
      padding: 24px;
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(20px);
      border-radius: 20px;
      border: 1px solid rgba(247,147,26,0.3);
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .header h1 {
      color: #ffffff;
      font-size: 1.5em;
      font-weight: 700;
      margin-bottom: 6px;
      letter-spacing: -0.02em;
    }
    .header h1 span { color: #f7931a; }
    .header p { color: #8b949e; font-size: 0.9em; }

    .stats-bar {
      display: flex;
      justify-content: center;
      gap: 16px;
      margin-bottom: 24px;
    }
    .stat-item {
      text-align: center;
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(10px);
      padding: 20px 32px;
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.06);
      flex: 1;
      max-width: 200px;
    }
    .stat-value { font-size: 2em; font-weight: 700; color: #f7931a; font-family: 'JetBrains Mono', monospace; }
    .stat-label { font-size: 0.8em; color: #8b949e; margin-top: 4px; font-weight: 500; }

    .card {
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(20px);
      border-radius: 20px;
      padding: 24px;
      margin-bottom: 20px;
      border: 1px solid rgba(255,255,255,0.06);
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .card h2 {
      color: #ffffff;
      font-size: 1.1em;
      font-weight: 600;
      margin-bottom: 20px;
      padding-bottom: 12px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
      letter-spacing: -0.02em;
    }

    .form-group { margin-bottom: 18px; }
    .form-group label {
      display: block;
      margin-bottom: 8px;
      color: #8b949e;
      font-size: 0.85em;
      font-weight: 500;
    }
    .form-group input, .form-group textarea, .form-group select {
      width: 100%;
      padding: 14px 16px;
      border: 2px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      background: rgba(15, 20, 25, 0.8);
      color: #e7e9ea;
      font-size: 0.95em;
      font-family: inherit;
      transition: all 0.2s ease;
    }
    .form-group input:focus, .form-group textarea:focus, .form-group select:focus {
      outline: none;
      border-color: #f7931a;
      box-shadow: 0 0 0 4px rgba(247,147,26,0.15);
    }
    .form-group textarea { resize: vertical; min-height: 100px; }

    .type-buttons {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
    }
    .type-btn {
      padding: 14px 10px;
      border: 2px solid rgba(255,255,255,0.08);
      border-radius: 14px;
      background: rgba(15, 20, 25, 0.8);
      color: #8b949e;
      cursor: pointer;
      text-align: center;
      transition: all 0.2s ease;
      font-family: inherit;
    }
    .type-btn:hover { border-color: rgba(247,147,26,0.5); background: rgba(247,147,26,0.08); }
    .type-btn.active { border-color: #f7931a; color: #f7931a; background: rgba(247,147,26,0.12); }
    .type-btn .icon { font-size: 1.6em; display: block; margin-bottom: 6px; }
    .type-btn .label { font-size: 0.8em; font-weight: 500; }

    .btn {
      width: 100%;
      padding: 16px;
      border: none;
      border-radius: 14px;
      font-size: 1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      font-family: inherit;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%);
      color: white;
      box-shadow: 0 4px 20px rgba(247,147,26,0.35);
    }
    .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 8px 30px rgba(247,147,26,0.45); }
    .btn-primary:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

    .result {
      margin-top: 16px;
      padding: 14px 16px;
      border-radius: 12px;
      display: none;
      font-weight: 500;
    }
    .result.success { display: block; background: rgba(34,197,94,0.12); border: 1px solid rgba(34,197,94,0.3); color: #4ade80; }
    .result.error { display: block; background: rgba(239,68,68,0.12); border: 1px solid rgba(239,68,68,0.3); color: #f87171; }

    .history { max-height: 320px; overflow-y: auto; }
    .history-item {
      padding: 14px 16px;
      background: rgba(15, 20, 25, 0.8);
      border-radius: 12px;
      margin-bottom: 10px;
      border-left: 4px solid #f7931a;
    }
    .history-item .time { font-size: 0.8em; color: #8b949e; }
    .history-item .title { font-weight: 600; color: #ffffff; margin-top: 4px; }
    .history-item .message { font-size: 0.85em; color: #8b949e; margin-top: 4px; line-height: 1.4; }
    .history-item.promo { border-left-color: #4ade80; }
    .history-item.warning { border-left-color: #fbbf24; }
    .history-item.urgent { border-left-color: #f87171; }

    .empty-state { text-align: center; color: #8b949e; padding: 40px; font-size: 0.95em; }

    .back-link {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      color: #8b949e;
      text-decoration: none;
      font-size: 0.9em;
      margin-bottom: 16px;
      transition: color 0.2s;
    }
    .back-link:hover { color: #f7931a; }

    @media (max-width: 480px) {
      body { padding: 12px; }
      .header { padding: 20px; border-radius: 16px; }
      .card { padding: 20px; border-radius: 16px; }
      .type-buttons { grid-template-columns: repeat(2, 1fr); }
      .stat-item { padding: 16px 20px; }
      .stat-value { font-size: 1.6em; }
    }
  </style>
</head>
<body>
  <div class="container">
    <a href="/admin/users" class="back-link">← Kembali ke Kelola User</a>
    <div class="header">
      <h1><span>Admin</span> Panel</h1>
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
  // Remove leading 0 or 62
  if (clean.startsWith('62')) clean = clean.substring(2)
  if (clean.startsWith('0')) clean = clean.substring(1)
  // Always return with 62 prefix for consistency with database
  return '62' + clean
}

// Helper: Check if user is valid (exists and not expired)
async function isUserValid(phone) {
  try {
    // Admin is always valid
    if (phone === 'admin') {
      return { valid: true, user: { name: 'Administrator', phone: 'admin', isAdmin: true } }
    }

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
  if (!phone) return res.json({ success: false, error: 'Nomor HP wajib diisi' })

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
    const jid = `${normalizedPhone}@s.whatsapp.net`
    await sock.sendMessage(jid, {
      text: `🔐 *Kode OTP Gold Price Monitor*\n\nKode verifikasi Anda: *${otp}*\n\nKode berlaku 5 menit.\nJangan bagikan kode ini kepada siapapun.`
    })

    pushLog(`OTP | Sent to +${normalizedPhone}`)
    res.json({ success: true, message: 'Kode OTP telah dikirim ke WhatsApp Anda' })
  } catch (e) {
    pushLog(`OTP | Failed to send to +${normalizedPhone}: ${e.message}`)
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

  pushLog(`OTP | User registered: +${normalizedPhone}`)
  res.json({ success: true, sessionId, user: userData })
})

// API: Login user
// Helper: Get default PIN (000000 for all users)
function getDefaultPin(phone) {
  return '000000'
}

// Helper: Simple hash PIN for security (not storing plain text)
function hashPin(pin) {
  // Simple hash using base64 encoding with salt
  const salt = 'goldmonitor2024'
  const combined = pin + salt
  return Buffer.from(combined).toString('base64')
}

// Helper: Verify PIN
function verifyPin(inputPin, storedHash) {
  return hashPin(inputPin) === storedHash
}

// API: Check user exists (step 1 of login)
app.post('/api/check-user', express.json(), async (req, res) => {
  const { phone } = req.body
  if (!phone) return res.json({ success: false, error: 'Nomor HP wajib diisi' })

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

  // Check if user has PIN set
  const pinData = await redis.hget(REDIS_KEYS.USER_PINS, normalizedPhone)
  let pinChanged = false
  if (pinData) {
    try {
      const parsed = typeof pinData === 'string' ? JSON.parse(pinData) : pinData
      pinChanged = parsed.pinChanged || false
    } catch (e) {}
  }

  res.json({
    success: true,
    user: { name: check.user.name },
    pinChanged // true if user already changed default PIN
  })
})

// API: Login with PIN (step 2 of login)
app.post('/api/login', express.json(), async (req, res) => {
  const { phone, pin } = req.body
  if (!phone) return res.json({ success: false, error: 'Nomor HP wajib diisi' })
  if (!pin) return res.json({ success: false, error: 'PIN wajib diisi' })

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

  // Check PIN
  const pinData = await redis.hget(REDIS_KEYS.USER_PINS, normalizedPhone)
  let storedPin = null
  let pinChanged = false

  if (pinData) {
    try {
      const parsed = typeof pinData === 'string' ? JSON.parse(pinData) : pinData
      storedPin = parsed.pin
      pinChanged = parsed.pinChanged || false
    } catch (e) {}
  }

  // If no PIN set, use default PIN (first 6 digits of phone)
  if (!storedPin) {
    const defaultPin = getDefaultPin(normalizedPhone)
    storedPin = hashPin(defaultPin)
    // Save default PIN to database
    await redis.hset(REDIS_KEYS.USER_PINS, {
      [normalizedPhone]: JSON.stringify({ pin: storedPin, pinChanged: false })
    })
    pinChanged = false
  }

  // Verify PIN
  if (!verifyPin(pin, storedPin)) {
    return res.json({ success: false, error: 'PIN salah. Silakan coba lagi.' })
  }

  // Check existing sessions for this user (max 2 devices)
  const allSessions = await redis.hgetall(REDIS_KEYS.SESSIONS) || {}
  const userSessions = []
  for (const [sessId, sessPhone] of Object.entries(allSessions)) {
    if (sessPhone === normalizedPhone) {
      userSessions.push(sessId)
    }
  }

  // If already 2 sessions, remove the oldest one (first in array)
  if (userSessions.length >= 2) {
    // Remove oldest session (FIFO - first in first out)
    await redis.hdel(REDIS_KEYS.SESSIONS, userSessions[0])
    pushLog(`Auth | User +${normalizedPhone} exceeded 2 devices, oldest session removed`)
  }

  // Create new session
  const sessionId = generateSessionId()
  await redis.hset(REDIS_KEYS.SESSIONS, { [sessionId]: normalizedPhone })

  res.json({
    success: true,
    sessionId,
    user: check.user,
    requirePinChange: !pinChanged // true if user must change PIN
  })
})

// API: Change PIN
app.post('/api/change-pin', express.json(), async (req, res) => {
  const { session, oldPin, newPin } = req.body

  if (!session) return res.json({ success: false, error: 'Session tidak valid' })
  if (!newPin || newPin.length !== 6 || !/^\d{6}$/.test(newPin)) {
    return res.json({ success: false, error: 'PIN baru harus 6 digit angka' })
  }

  const phone = await redis.hget(REDIS_KEYS.SESSIONS, session)
  if (!phone) return res.json({ success: false, error: 'Session tidak valid' })

  // Get current PIN
  const pinData = await redis.hget(REDIS_KEYS.USER_PINS, phone)
  let storedPin = null
  let pinChanged = false

  if (pinData) {
    try {
      const parsed = typeof pinData === 'string' ? JSON.parse(pinData) : pinData
      storedPin = parsed.pin
      pinChanged = parsed.pinChanged || false
    } catch (e) {}
  }

  // If PIN already changed, verify old PIN
  if (pinChanged && oldPin) {
    if (!verifyPin(oldPin, storedPin)) {
      return res.json({ success: false, error: 'PIN lama salah' })
    }
  }

  // Save new PIN
  const newPinHash = hashPin(newPin)
  await redis.hset(REDIS_KEYS.USER_PINS, {
    [phone]: JSON.stringify({ pin: newPinHash, pinChanged: true })
  })

  pushLog(`Auth | User +${phone} changed PIN`)
  res.json({ success: true, message: 'PIN berhasil diubah' })
})

// API: Check if PIN needs to be changed
app.get('/api/check-pin-status', async (req, res) => {
  const session = req.query.session
  if (!session) return res.json({ success: false })

  const phone = await redis.hget(REDIS_KEYS.SESSIONS, session)
  if (!phone) return res.json({ success: false })

  // Admin doesn't need PIN
  if (phone === 'admin') {
    return res.json({ success: true, pinChanged: true, requirePinChange: false })
  }

  const pinData = await redis.hget(REDIS_KEYS.USER_PINS, phone)
  let pinChanged = false

  if (pinData) {
    try {
      const parsed = typeof pinData === 'string' ? JSON.parse(pinData) : pinData
      pinChanged = parsed.pinChanged || false
    } catch (e) {}
  }

  res.json({ success: true, pinChanged, requirePinChange: !pinChanged })
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

// API: Admin Reset User PIN to default (000000)
app.post('/api/admin/reset-pin', express.json(), async (req, res) => {
  const { password, phone } = req.body
  if (password !== ADMIN_PASSWORD) {
    return res.json({ success: false, error: 'Password admin salah' })
  }
  if (!phone) {
    return res.json({ success: false, error: 'Nomor HP wajib diisi' })
  }

  const normalizedPhone = normalizePhone(phone)

  // Check if user exists
  const userData = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)
  if (!userData) {
    return res.json({ success: false, error: 'User tidak ditemukan' })
  }

  // Reset PIN to default (000000)
  const defaultPinHash = hashPin('000000')
  await redis.hset(REDIS_KEYS.USER_PINS, {
    [normalizedPhone]: JSON.stringify({ pin: defaultPinHash, pinChanged: false })
  })

  pushLog(`Admin | Reset PIN for user +${normalizedPhone}`)
  res.json({ success: true, message: 'PIN berhasil direset ke 000000' })
})

// Helper: Generate login token
function generateLoginToken() {
  return Math.random().toString(36).substr(2, 12) + Date.now().toString(36)
}

// API: Request login link via WhatsApp
app.post('/api/user/request-login', express.json(), async (req, res) => {
  const { phone } = req.body
  if (!phone) return res.json({ success: false, error: 'Nomor HP wajib diisi' })

  const normalizedPhone = normalizePhone(phone)

  // Check if user is blocked
  const blocked = await redis.hget(REDIS_KEYS.BLOCKED_USERS, normalizedPhone)
  if (blocked) {
    return res.json({ success: false, error: 'Akun diblokir. Hubungi admin untuk membuka blokir.' })
  }

  // Check if user exists and valid
  const check = await isUserValid(normalizedPhone)
  if (!check.valid) {
    if (check.reason === 'not_found') {
      return res.json({ success: false, error: 'Nomor tidak terdaftar. Hubungi admin untuk mendaftar.' })
    }
    if (check.reason === 'expired') {
      return res.json({ success: false, error: 'Akun sudah expired. Hubungi admin untuk perpanjang.' })
    }
    return res.json({ success: false, error: 'Terjadi kesalahan' })
  }

  // Check if WhatsApp is connected
  if (!sock || !isReady) {
    return res.json({ success: false, error: 'WhatsApp tidak terhubung. Coba lagi nanti.' })
  }

  // Generate login token (valid for 5 minutes)
  const token = generateLoginToken()
  const tokenData = {
    phone: normalizedPhone,
    expires: Date.now() + 5 * 60 * 1000 // 5 minutes
  }

  await redis.hset(REDIS_KEYS.LOGIN_TOKENS, { [token]: JSON.stringify(tokenData) })

  // Get base URL from request
  const protocol = req.headers['x-forwarded-proto'] || 'https'
  const host = req.headers.host
  const loginUrl = `${protocol}://${host}/auth/${token}`

  // Send login link via WhatsApp
  try {
    const jid = `${normalizedPhone}@s.whatsapp.net`
    await sock.sendMessage(jid, {
      text: `🔐 *Login Gold Price Monitor*\n\nHalo ${check.user?.name || 'User'}!\n\nKlik link berikut untuk masuk:\n${loginUrl}\n\n⏰ Link berlaku 5 menit.\n⚠️ Jangan bagikan link ini kepada siapapun.`
    })

    pushLog(`Auth | Login link sent to +${normalizedPhone}`)
    res.json({ success: true, message: 'Link login telah dikirim ke WhatsApp Anda' })
  } catch (e) {
    pushLog(`Auth | Failed to send login link to +${normalizedPhone}: ${e.message}`)
    res.json({ success: false, error: 'Gagal mengirim link. Pastikan nomor WhatsApp aktif.' })
  }
})

// API: Save push subscription
app.post('/api/push-subscribe', express.json(), async (req, res) => {
  const { session, subscription } = req.body
  if (!session || !subscription) return res.json({ success: false })

  const phone = await redis.hget(REDIS_KEYS.SESSIONS, session)
  if (!phone) return res.json({ success: false, error: 'Invalid session' })

  await redis.hset(REDIS_KEYS.PUSH_SUBS, { [phone]: JSON.stringify(subscription) })
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
    const [users, blockedUsers, pinData] = await Promise.all([
      redis.hgetall(REDIS_KEYS.USERS),
      redis.hgetall(REDIS_KEYS.BLOCKED_USERS),
      redis.hgetall(REDIS_KEYS.USER_PINS)
    ])
    const result = []

    for (const [phone, data] of Object.entries(users || {})) {
      const user = typeof data === 'string' ? JSON.parse(data) : data
      const hasPushSub = await redis.hget(REDIS_KEYS.PUSH_SUBS, phone)
      const isBlocked = !!blockedUsers?.[phone]

      // Check PIN status
      let pinChanged = false
      if (pinData && pinData[phone]) {
        try {
          const pinInfo = typeof pinData[phone] === 'string' ? JSON.parse(pinData[phone]) : pinData[phone]
          pinChanged = pinInfo.pinChanged || false
        } catch (e) { /* ignore */ }
      }

      result.push({
        phone,
        ...user,
        hasPushSubscription: !!hasPushSub,
        isBlocked,
        pinChanged
      })
    }

    res.json({ success: true, users: result })
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Block user
app.post('/api/admin/users/block', express.json(), async (req, res) => {
  const { password, phone, reason } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phone) return res.json({ success: false, error: 'Phone required' })

  const normalizedPhone = normalizePhone(phone)
  const blockData = {
    blockedAt: Date.now(),
    reason: reason || 'Blocked by admin'
  }

  await redis.hset(REDIS_KEYS.BLOCKED_USERS, { [normalizedPhone]: JSON.stringify(blockData) })

  // Also remove all sessions for this user
  const sessions = await redis.hgetall(REDIS_KEYS.SESSIONS)
  for (const [sessId, sessPhone] of Object.entries(sessions || {})) {
    if (sessPhone === normalizedPhone) {
      await redis.hdel(REDIS_KEYS.SESSIONS, sessId)
    }
  }

  pushLog(`Admin | Blocked user +${normalizedPhone}`)
  res.json({ success: true })
})

// Admin: Unblock user
app.post('/api/admin/users/unblock', express.json(), async (req, res) => {
  const { password, phone } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phone) return res.json({ success: false, error: 'Phone required' })

  const normalizedPhone = normalizePhone(phone)
  await redis.hdel(REDIS_KEYS.BLOCKED_USERS, normalizedPhone)

  pushLog(`Admin | Unblocked user +${normalizedPhone}`)
  res.json({ success: true })
})

// Admin: Add user
app.post('/api/admin/users', express.json(), async (req, res) => {
  const { password, phone, name, expiredDays, expiredTimestamp } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phone) return res.json({ success: false, error: 'Nomor WA wajib diisi' })

  const normalizedPhone = normalizePhone(phone)
  const now = Date.now()

  // Support both expiredTimestamp (from date picker) and expiredDays
  let expired = null
  if (expiredTimestamp) {
    expired = expiredTimestamp
  } else if (expiredDays) {
    expired = now + (expiredDays * 24 * 60 * 60 * 1000)
  }

  const userData = {
    name: name || 'Member ' + normalizedPhone.substring(2),
    createdAt: now,
    expired: expired
  }

  await redis.hset(REDIS_KEYS.USERS, { [normalizedPhone]: JSON.stringify(userData) })

  pushLog(`Admin | Added user +${normalizedPhone}, expired: ${expired ? new Date(expired).toLocaleDateString('id-ID') : 'Lifetime'}`)

  res.json({ success: true, user: { phone: normalizedPhone, ...userData } })
})

// Admin: Bulk import users
app.post('/api/admin/users/bulk', express.json(), async (req, res) => {
  const { password, phones } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phones || !Array.isArray(phones)) return res.json({ success: false, error: 'phones array required' })

  let added = 0
  let skipped = 0
  const now = Date.now()

  for (const phone of phones) {
    const normalizedPhone = normalizePhone(phone)
    if (!normalizedPhone || normalizedPhone.length < 9) {
      skipped++
      continue
    }

    // Check if exists
    const existing = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)
    if (existing) {
      skipped++
      continue
    }

    const userData = JSON.stringify({
      name: 'Member ' + normalizedPhone,
      createdAt: now,
      expired: null,
      source: 'bulk_import'
    })

    await redis.hset(REDIS_KEYS.USERS, { [normalizedPhone]: userData })
    added++
  }

  pushLog(`Admin | Bulk import: ${added} added, ${skipped} skipped`)
  res.json({ success: true, added, skipped, total: phones.length })
})

// Admin: Update user
app.put('/api/admin/users', express.json(), async (req, res) => {
  const { password, phone, name, expiredDays, addDays, expiredTimestamp } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  const normalizedPhone = normalizePhone(phone)
  const existing = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)

  if (!existing) return res.json({ success: false, error: 'User tidak ditemukan' })

  const user = typeof existing === 'string' ? JSON.parse(existing) : existing

  if (name) user.name = name

  // Handle expired timestamp from date picker
  if (expiredTimestamp) {
    user.expired = expiredTimestamp
  } else if (expiredDays !== undefined) {
    user.expired = expiredDays ? Date.now() + (expiredDays * 24 * 60 * 60 * 1000) : null
  } else if (addDays) {
    const base = user.expired && user.expired > Date.now() ? user.expired : Date.now()
    user.expired = base + (addDays * 24 * 60 * 60 * 1000)
  }

  await redis.hset(REDIS_KEYS.USERS, { [normalizedPhone]: JSON.stringify(user) })

  pushLog(`Admin | Updated user +${normalizedPhone}: name=${user.name}, expired=${user.expired ? new Date(user.expired).toLocaleDateString('id-ID') : 'Lifetime'}`)

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

// Admin: Kick user from WhatsApp group AND delete from database
app.post('/api/admin/users/kick', express.json(), async (req, res) => {
  const { password, phone } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  if (!phone) return res.json({ success: false, error: 'Nomor wajib diisi' })

  const normalizedPhone = normalizePhone(phone)
  const jid = `${normalizedPhone}@s.whatsapp.net`

  try {
    // Check if we have a monitored group
    if (!monitoredGroupId) {
      return res.json({ success: false, error: 'Belum ada grup yang di-monitor. Set grup terlebih dahulu.' })
    }

    // Check if WhatsApp is connected
    if (!sock) {
      return res.json({ success: false, error: 'WhatsApp tidak terhubung' })
    }

    // Try to kick from WhatsApp group
    let kickedFromGroup = false
    try {
      await sock.groupParticipantsUpdate(monitoredGroupId, [jid], 'remove')
      kickedFromGroup = true
      pushLog(`WA | Kicked +${normalizedPhone} from group`)

      // Send kick notification to user
      try {
        await sock.sendMessage(jid, {
          text: `❌ *ANDA TELAH DI-KICK*\n\nAnda telah dikeluarkan dari grup Gold Price Monitor.\n\nJika ada pertanyaan, hubungi admin:\nhttps://wa.me/6289654454210`
        })
      } catch (msgErr) {
        console.log('Failed to send kick message:', msgErr.message)
      }
    } catch (kickError) {
      // User might not be in group, or bot is not admin
      pushLog(`WA | Failed to kick +${normalizedPhone}: ${kickError.message}`)
      // Continue to delete user even if kick fails
    }

    // Delete user from database
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

    pushLog(`Admin | User +${normalizedPhone} deleted (kicked: ${kickedFromGroup})`)

    res.json({
      success: true,
      kickedFromGroup,
      message: kickedFromGroup
        ? 'User berhasil di-kick dari grup dan dihapus dari database'
        : 'User dihapus dari database (gagal kick dari grup - mungkin bukan admin atau user tidak di grup)'
    })
  } catch (e) {
    pushLog(`Admin | Kick error: ${e.message}`)
    res.json({ success: false, error: e.message })
  }
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

// Admin: Force logout all users (clear all sessions)
app.post('/api/admin/force-logout-all', express.json(), async (req, res) => {
  const { password } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  try {
    await redis.del(REDIS_KEYS.SESSIONS)
    await redis.del(REDIS_KEYS.LOGIN_TOKENS)

    // Broadcast ke semua client untuk logout
    broadcastSSE({ type: 'force_logout', message: 'Session expired, please login again' })

    pushLog(`Admin | Force logout all users`)
    res.json({ success: true, message: 'Semua user berhasil di-logout' })
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

// ==================== SOUND SETTINGS ====================

// Get sound settings (public - for monitoring page)
app.get('/api/sound-settings', async (_req, res) => {
  try {
    const settings = await redis.get(REDIS_KEYS.SOUND_SETTINGS)
    if (settings) {
      const parsed = typeof settings === 'string' ? JSON.parse(settings) : settings
      res.json({ success: true, settings: parsed })
    } else {
      res.json({ success: true, settings: { soundUp: '', soundDown: '' } })
    }
  } catch (e) {
    res.json({ success: false, error: e.message })
  }
})

// Admin: Update sound settings
app.post('/api/admin/sound-settings', express.json(), async (req, res) => {
  const { password, soundUp, soundDown } = req.body
  if (password !== ADMIN_PASSWORD) return res.json({ success: false, error: 'Unauthorized' })

  try {
    const settings = { soundUp: soundUp || '', soundDown: soundDown || '' }
    await redis.set(REDIS_KEYS.SOUND_SETTINGS, JSON.stringify(settings))

    // Broadcast to all clients to update their sounds
    broadcastSSE({ type: 'sound_update', settings })

    res.json({ success: true, settings })
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


// ==================== REGISTRATION ENDPOINTS ====================

// Register endpoint - user submit pendaftaran
app.post('/api/register', async (req, res) => {
  try {
    const { phone, name } = req.body

    if (!phone || !name) {
      return res.json({ success: false, message: 'Nama dan nomor HP wajib diisi' })
    }

    // Normalize phone
    let normalizedPhone = phone.replace(/\D/g, '')
    if (normalizedPhone.startsWith('0')) normalizedPhone = '62' + normalizedPhone.substring(1)
    if (!normalizedPhone.startsWith('62')) normalizedPhone = '62' + normalizedPhone

    // Check if already registered
    const existing = await redis.hget(REDIS_KEYS.USERS, normalizedPhone)
    if (existing) {
      return res.json({ success: false, message: 'Nomor ini sudah terdaftar. Silakan login.' })
    }

    // Check if already pending
    const existingPending = await redis.hget(REDIS_KEYS.PENDING_REGISTRATIONS, normalizedPhone)
    if (existingPending) {
      return res.json({ success: false, message: 'Pendaftaran Anda sedang menunggu persetujuan admin.' })
    }

    // Add to pending (stored in Redis)
    await redis.hset(REDIS_KEYS.PENDING_REGISTRATIONS, {
      [normalizedPhone]: JSON.stringify({
        name: name,
        phone: normalizedPhone,
        timestamp: Date.now()
      })
    })

    // Send notification to all admin phones via WhatsApp
    if (isReady && sock) {
      for (const adminPhone of ADMIN_PHONES) {
        try {
          const adminJid = adminPhone + '@s.whatsapp.net'
          await sock.sendMessage(adminJid, {
            text: `🔔 *PENDAFTARAN BARU*\n\nNama: *${name}*\nNo HP: ${normalizedPhone}\n\nSilakan ACC di menu admin:\nhttps://ts.muhamadaliyudin.xyz/admin/users`
          })
          pushLog(`REGISTER | Notification sent to admin ${adminPhone} for ${normalizedPhone}`)
        } catch (e) {
          pushLog(`REGISTER | Failed to send admin notification to ${adminPhone}: ${e.message}`)
        }
      }
    }

    pushLog(`REGISTER | New registration: ${name} (${normalizedPhone})`)

    res.json({
      success: true,
      message: 'Pendaftaran berhasil dikirim! Tunggu persetujuan admin.'
    })
  } catch (e) {
    console.error('Register error:', e)
    res.json({ success: false, message: 'Terjadi kesalahan. Coba lagi.' })
  }
})

// Get pending registrations (admin only)
app.get('/api/pending-registrations', async (req, res) => {
  try {
    const all = await redis.hgetall(REDIS_KEYS.PENDING_REGISTRATIONS)

    // Debug log
    console.log('Pending raw data type:', typeof all, Array.isArray(all))
    console.log('Pending raw data:', JSON.stringify(all).substring(0, 500))

    if (!all) {
      return res.json({ registrations: [] })
    }

    const list = []

    // Upstash returns object {key: value, key2: value2}
    // Or could be array [key, value, key2, value2]
    if (Array.isArray(all)) {
      // Handle array format [key, val, key, val...]
      for (let i = 0; i < all.length; i += 2) {
        try {
          const data = all[i + 1]
          if (data) {
            const parsed = typeof data === 'string' ? JSON.parse(data) : data
            list.push(parsed)
          }
        } catch (e) {}
      }
    } else if (typeof all === 'object') {
      // Handle object format
      for (const data of Object.values(all)) {
        try {
          const parsed = typeof data === 'string' ? JSON.parse(data) : data
          list.push(parsed)
        } catch (e) {}
      }
    }

    res.json({ registrations: list })
  } catch (e) {
    console.error('Get pending error:', e)
    res.json({ registrations: [] })
  }
})

// Check if user exists in database
app.get('/api/check-user/:phone', async (req, res) => {
  try {
    const phone = req.params.phone
    const userData = await redis.hget(REDIS_KEYS.USERS, phone)
    if (userData) {
      const user = typeof userData === 'string' ? JSON.parse(userData) : userData
      res.json({ exists: true, user })
    } else {
      res.json({ exists: false })
    }
  } catch (e) {
    res.json({ error: e.message })
  }
})

// Simple test with individual set commands
app.get('/api/test-pending-add', async (req, res) => {
  try {
    const key = 'gold:pending_reg_v2'

    // Use set instead of hset - store as simple string with phone as part of key
    const testData = [
      { name: 'Ahmad Wijaya', phone: '6281234567890', timestamp: Date.now() },
      { name: 'Budi Santoso', phone: '6282345678901', timestamp: Date.now() - 60000 },
      { name: 'Citra Dewi', phone: '6283456789012', timestamp: Date.now() - 120000 }
    ]

    // Clear first using del
    await redis.del(key)

    // Try different hset syntax
    for (const data of testData) {
      // Upstash might need: hset(key, {field: value})
      await redis.hset(key, { [data.phone]: JSON.stringify(data) })
    }

    const verify = await redis.hgetall(key)

    res.json({
      success: true,
      testData,
      verifyType: typeof verify,
      verifyIsArray: Array.isArray(verify),
      verify
    })
  } catch (e) {
    res.json({ error: e.message, stack: e.stack })
  }
})

// Force clear pending registrations
app.get('/api/force-clear-pending', async (req, res) => {
  try {
    // Delete the entire key
    const deleted = await redis.del('gold:pending_registrations')

    // Verify it's gone
    const check = await redis.hgetall('gold:pending_registrations')

    res.json({ deleted: deleted, remaining: check })
  } catch (e) {
    res.json({ error: e.message })
  }
})

// Debug endpoint to see raw redis data
app.get('/api/debug-pending', async (req, res) => {
  try {
    const raw = await redis.hgetall(REDIS_KEYS.PENDING_REGISTRATIONS)
    res.json({
      type: typeof raw,
      isArray: Array.isArray(raw),
      keys: raw ? Object.keys(raw) : [],
      raw: raw
    })
  } catch (e) {
    res.json({ error: e.message })
  }
})

// Clear and add test pending registrations (admin only)
app.post('/api/reset-pending-test', async (req, res) => {
  try {
    // First get all keys and delete them individually
    const all = await redis.hgetall(REDIS_KEYS.PENDING_REGISTRATIONS)
    if (all) {
      for (const key of Object.keys(all)) {
        await redis.hdel(REDIS_KEYS.PENDING_REGISTRATIONS, key)
      }
    }

    // Also try del
    await redis.del(REDIS_KEYS.PENDING_REGISTRATIONS)

    // Add test data - use hset with explicit key-value pairs
    const testData = [
      { name: 'Ahmad Wijaya', phone: '6281234567890', timestamp: Date.now() },
      { name: 'Budi Santoso', phone: '6282345678901', timestamp: Date.now() - 60000 },
      { name: 'Citra Dewi', phone: '6283456789012', timestamp: Date.now() - 120000 }
    ]

    for (const data of testData) {
      const jsonData = JSON.stringify(data)
      await redis.hset(REDIS_KEYS.PENDING_REGISTRATIONS, { [data.phone]: jsonData })
      console.log('Added:', data.phone, '=', jsonData)
    }

    // Verify
    const verify = await redis.hgetall(REDIS_KEYS.PENDING_REGISTRATIONS)
    console.log('Verify after add:', verify)

    res.json({ success: true, message: 'Test data added', count: testData.length, verify: verify })
  } catch (e) {
    console.error('Reset error:', e)
    res.json({ success: false, message: e.message })
  }
})

// Approve registration (admin only)
app.post('/api/approve-registration', async (req, res) => {
  try {
    const { phone } = req.body
    console.log('Approve request for phone:', phone)

    const pendingData = await redis.hget(REDIS_KEYS.PENDING_REGISTRATIONS, phone)
    console.log('Pending data:', pendingData, typeof pendingData)

    if (!pendingData) {
      return res.json({ success: false, message: 'Pendaftaran tidak ditemukan' })
    }

    // Handle both string and object formats
    const registration = typeof pendingData === 'string' ? JSON.parse(pendingData) : pendingData

    // Create user
    const userData = {
      phone: phone,
      name: registration.name,
      createdAt: new Date().toISOString(),
      active: true
    }

    // Use correct Upstash hset syntax
    await redis.hset(REDIS_KEYS.USERS, { [phone]: JSON.stringify(userData) })

    // Remove from pending (Redis)
    await redis.hdel(REDIS_KEYS.PENDING_REGISTRATIONS, phone)

    // Send approval message to user
    if (isReady && sock) {
      try {
        const userJid = phone + '@s.whatsapp.net'
        await sock.sendMessage(userJid, {
          text: `✅ *PENDAFTARAN DISETUJUI*\n\nHalo ${registration.name}!\n\nPendaftaran Anda telah disetujui.\nSilakan login di:\nhttps://ts.muhamadaliyudin.xyz/login\n\nGunakan nomor ini untuk login.`
        })
      } catch (e) {}
    }

    pushLog(`REGISTER | Approved: ${registration.name} (${phone})`)

    res.json({ success: true, message: 'Pendaftaran disetujui' })
  } catch (e) {
    res.json({ success: false, message: 'Gagal menyetujui pendaftaran: ' + e.message })
  }
})

// Reject registration (admin only)
app.post('/api/reject-registration', async (req, res) => {
  try {
    const { phone, reason } = req.body
    console.log('Reject request for phone:', phone)

    const pendingData = await redis.hget(REDIS_KEYS.PENDING_REGISTRATIONS, phone)
    console.log('Pending data for reject:', pendingData, typeof pendingData)

    if (!pendingData) {
      return res.json({ success: false, message: 'Pendaftaran tidak ditemukan' })
    }

    // Handle both string and object formats
    const registration = typeof pendingData === 'string' ? JSON.parse(pendingData) : pendingData

    // Remove from pending (Redis)
    await redis.hdel(REDIS_KEYS.PENDING_REGISTRATIONS, phone)

    // Send rejection message to user
    if (isReady && sock) {
      try {
        const userJid = phone + '@s.whatsapp.net'
        await sock.sendMessage(userJid, {
          text: `❌ *PENDAFTARAN DITOLAK*\n\nMaaf ${registration.name},\n\nPendaftaran Anda tidak disetujui.${reason ? '\nAlasan: ' + reason : ''}\n\nSilakan hubungi admin untuk informasi lebih lanjut.`
        })
      } catch (e) {}
    }

    pushLog(`REGISTER | Rejected: ${registration.name} (${phone})`)

    res.json({ success: true, message: 'Pendaftaran ditolak' })
  } catch (e) {
    res.json({ success: false, message: 'Gagal menolak pendaftaran: ' + e.message })
  }
})


// ==================== ADMIN PHONES MANAGEMENT ====================

// Get admin phones
app.get('/api/admin-phones', (req, res) => {
  res.json({ success: true, phones: ADMIN_PHONES })
})

// Update admin phones
app.post('/api/admin-phones', (req, res) => {
  try {
    const { phones } = req.body
    if (!Array.isArray(phones) || phones.length === 0) {
      return res.json({ success: false, message: 'Minimal 1 nomor admin' })
    }

    // Normalize phones
    ADMIN_PHONES = phones.map(p => {
      let normalized = p.replace(/\D/g, '')
      if (normalized.startsWith('0')) normalized = '62' + normalized.substring(1)
      if (!normalized.startsWith('62')) normalized = '62' + normalized
      return normalized
    }).filter(p => p.length >= 10)

    if (ADMIN_PHONES.length === 0) {
      ADMIN_PHONES = ['62895701692525'] // Fallback
      return res.json({ success: false, message: 'Nomor tidak valid' })
    }

    pushLog(`ADMIN | Admin phones updated: ${ADMIN_PHONES.join(', ')}`)
    res.json({ success: true, phones: ADMIN_PHONES })
  } catch (e) {
    res.json({ success: false, message: e.message })
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
  <meta name="theme-color" content="#0a0e13">
  <link rel="manifest" href="/manifest.json">
  <link rel="icon" href="/icon.png">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <title>Login - Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: linear-gradient(145deg, #0a0e13 0%, #131921 50%, #0f1419 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      color: #e7e9ea;
      position: relative;
      overflow: hidden;
    }
    body::before {
      content: '';
      position: absolute;
      top: -50%;
      left: -50%;
      width: 200%;
      height: 200%;
      background: radial-gradient(circle at 30% 20%, rgba(247,147,26,0.08) 0%, transparent 50%),
                  radial-gradient(circle at 70% 80%, rgba(247,147,26,0.05) 0%, transparent 40%);
      animation: float 20s ease-in-out infinite;
      pointer-events: none;
    }
    @keyframes float {
      0%, 100% { transform: translate(0, 0) rotate(0deg); }
      50% { transform: translate(-2%, 2%) rotate(1deg); }
    }
    .container {
      width: 100%;
      max-width: 420px;
      text-align: center;
      position: relative;
      z-index: 1;
    }
    .card {
      background: rgba(20, 26, 34, 0.9);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-radius: 24px;
      padding: 40px 32px;
      border: 1px solid rgba(255,255,255,0.08);
      box-shadow: 0 25px 80px rgba(0,0,0,0.5),
                  0 0 0 1px rgba(255,255,255,0.05) inset;
    }
    .logo-container { margin-bottom: 28px; }
    .icon {
      width: 88px;
      height: 88px;
      margin: 0 auto 16px;
      border-radius: 22px;
      overflow: hidden;
      box-shadow: 0 12px 40px rgba(247,147,26,0.35);
      border: 2px solid rgba(247,147,26,0.3);
      transition: transform 0.3s ease;
    }
    .icon:hover { transform: scale(1.05); }
    .icon img { width: 100%; height: 100%; object-fit: cover; }
    h1 { color: #ffffff; font-size: 1.6em; font-weight: 700; margin-bottom: 8px; letter-spacing: -0.02em; }
    h1 span { color: #f7931a; }
    .subtitle { color: #8b949e; font-size: 0.9em; margin-bottom: 32px; line-height: 1.5; font-weight: 400; }
    .form-group { margin-bottom: 20px; text-align: left; }
    .form-group label { display: block; color: #8b949e; font-size: 0.85em; margin-bottom: 10px; font-weight: 500; }
    .form-group input {
      width: 100%;
      padding: 16px 18px;
      border: 2px solid rgba(255,255,255,0.08);
      border-radius: 14px;
      background: rgba(15, 20, 25, 0.8);
      color: #e7e9ea;
      font-size: 1em;
      font-family: inherit;
      transition: all 0.2s ease;
    }
    .form-group input:focus {
      outline: none;
      border-color: #f7931a;
      background: rgba(15, 20, 25, 1);
      box-shadow: 0 0 0 4px rgba(247,147,26,0.15);
    }
    .form-group input::placeholder { color: #4a5568; }
    .btn {
      width: 100%;
      padding: 16px;
      border: none;
      border-radius: 14px;
      font-size: 1em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-bottom: 12px;
      font-family: inherit;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%);
      color: white;
      box-shadow: 0 4px 20px rgba(247,147,26,0.35);
    }
    .btn-primary:hover:not(:disabled) { transform: translateY(-2px); box-shadow: 0 8px 30px rgba(247,147,26,0.45); }
    .btn-primary:active:not(:disabled) { transform: translateY(0); }
    .btn-primary:disabled { opacity: 0.6; cursor: not-allowed; transform: none; }
    .btn-secondary {
      background: rgba(255,255,255,0.08);
      color: #8b949e;
      border: 1px solid rgba(255,255,255,0.1);
    }
    .btn-secondary:hover:not(:disabled) { background: rgba(255,255,255,0.12); color: #e7e9ea; }
    .message {
      padding: 14px 16px;
      border-radius: 12px;
      margin-bottom: 20px;
      font-size: 0.9em;
      display: none;
      text-align: left;
      font-weight: 500;
    }
    .message.error { background: rgba(239,68,68,0.12); border: 1px solid rgba(239,68,68,0.3); color: #f87171; display: block; }
    .message.success { background: rgba(34,197,94,0.12); border: 1px solid rgba(34,197,94,0.3); color: #4ade80; display: block; }
    .message.info { background: rgba(247,147,26,0.12); border: 1px solid rgba(247,147,26,0.3); color: #f7931a; display: block; }
    .phone-prefix {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .phone-prefix span {
      background: linear-gradient(135deg, rgba(247,147,26,0.2), rgba(247,147,26,0.1));
      padding: 16px 14px;
      border-radius: 14px;
      color: #f7931a;
      font-weight: 600;
      border: 1px solid rgba(247,147,26,0.2);
      font-size: 0.95em;
    }
    .phone-prefix input { flex: 1; }
    .loading {
      display: inline-block;
      width: 20px;
      height: 20px;
      border: 2px solid rgba(255,255,255,0.3);
      border-radius: 50%;
      border-top-color: #fff;
      animation: spin 0.8s linear infinite;
      margin-right: 10px;
      vertical-align: middle;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .pin-input-container {
      display: flex;
      gap: 10px;
      justify-content: center;
      margin-bottom: 20px;
    }
    .pin-input {
      width: 48px;
      height: 56px;
      text-align: center;
      font-size: 1.5em;
      font-weight: 700;
      border: 2px solid rgba(255,255,255,0.08);
      border-radius: 12px;
      background: rgba(15, 20, 25, 0.8);
      color: #f7931a;
      font-family: 'JetBrains Mono', monospace;
      transition: all 0.2s ease;
    }
    .pin-input:focus {
      outline: none;
      border-color: #f7931a;
      background: rgba(15, 20, 25, 1);
      box-shadow: 0 0 0 4px rgba(247,147,26,0.15);
    }
    .pin-note {
      background: rgba(247,147,26,0.08);
      border: 1px solid rgba(247,147,26,0.2);
      border-radius: 12px;
      padding: 14px 16px;
      margin-bottom: 20px;
      font-size: 0.85em;
      color: #f7931a;
      text-align: left;
    }
    .user-info {
      background: rgba(74,222,128,0.08);
      border: 1px solid rgba(74,222,128,0.2);
      border-radius: 12px;
      padding: 14px 16px;
      margin-bottom: 20px;
      text-align: left;
    }
    .user-info .name { color: #4ade80; font-weight: 600; font-size: 1.1em; }
    .user-info .phone { color: #8b949e; font-size: 0.9em; margin-top: 4px; }
    .step-indicator {
      display: flex;
      justify-content: center;
      gap: 8px;
      margin-bottom: 24px;
    }
    .step {
      width: 32px;
      height: 4px;
      border-radius: 2px;
      background: rgba(255,255,255,0.1);
      transition: all 0.3s ease;
    }
    .step.active { background: #f7931a; width: 48px; }
    .step.completed { background: #4ade80; }
    .footer-text { margin-top: 24px; font-size: 0.8em; color: #4a5568; }
    .footer-text a { color: #f7931a; text-decoration: none; }
    /* Modal for PIN Change */
    .modal-overlay {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(0,0,0,0.8);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .modal-overlay.show { display: flex; }
    .modal {
      background: rgba(20, 26, 34, 0.98);
      border-radius: 24px;
      padding: 32px;
      max-width: 400px;
      width: 100%;
      border: 1px solid rgba(255,255,255,0.1);
      box-shadow: 0 25px 80px rgba(0,0,0,0.5);
    }
    .modal h2 { color: #f7931a; font-size: 1.3em; margin-bottom: 12px; }
    .modal p { color: #8b949e; font-size: 0.9em; line-height: 1.6; margin-bottom: 24px; }
    .modal .warning {
      background: rgba(239,68,68,0.1);
      border: 1px solid rgba(239,68,68,0.2);
      border-radius: 10px;
      padding: 12px;
      margin-bottom: 20px;
      color: #f87171;
      font-size: 0.85em;
    }
    @media (max-width: 480px) {
      .card { padding: 32px 24px; border-radius: 20px; }
      .icon { width: 72px; height: 72px; }
      h1 { font-size: 1.4em; }
      .subtitle { font-size: 0.85em; }
      .form-group input { padding: 14px 16px; }
      .btn { padding: 14px; }
      .pin-input { width: 42px; height: 50px; font-size: 1.3em; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <div class="logo-container">
        <div class="icon">
          <img src="/icon.png" alt="Gold Monitor">
        </div>
        <h1><span>Gold</span> Price Monitor</h1>
        <p class="subtitle">Pantau harga emas real-time dengan akurat</p>
      </div>

      <!-- Step Indicator -->
      <div class="step-indicator">
        <div class="step active" id="step1"></div>
        <div class="step" id="step2"></div>
      </div>

      <div id="message" class="message"></div>

      <!-- Step 1: Phone Number -->
      <div id="phoneForm">
        <div class="form-group">
          <label>Nomor WhatsApp</label>
          <div class="phone-prefix">
            <span>+62</span>
            <input type="tel" id="phoneInput" placeholder="8xxxxxxxxxx" maxlength="12" autocomplete="tel">
          </div>
        </div>
        <button class="btn btn-primary" id="checkBtn" onclick="checkUser()">
          Masuk ke Akun
        </button>
      </div>

      <!-- Step 2: PIN Input -->
      <div id="pinForm" style="display:none;">
        <div class="user-info" id="userInfo">
          <div class="name" id="userName">-</div>
          <div class="phone" id="userPhone">+62xxx</div>
        </div>

        <div class="form-group" style="text-align:center;">
          <label style="text-align:center;">Masukkan PIN 6 Digit</label>
          <div class="pin-input-container">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
            <input type="password" class="pin-input" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          </div>
        </div>

        <div class="pin-note">
          PIN default: 000000
        </div>

        <button class="btn btn-primary" id="loginBtn" onclick="submitLogin()">
          Masuk
        </button>
        <button class="btn btn-secondary" onclick="backToPhone()">
          Ganti Nomor
        </button>
      </div>

    </div>
  </div>

  <!-- Modal: Change PIN (Required) -->
  <div class="modal-overlay" id="changePinModal">
    <div class="modal">
      <h2>Ganti PIN Anda</h2>
      <p>Untuk keamanan akun, Anda wajib mengganti PIN default sebelum melanjutkan.</p>
      <div class="warning">
        Anda tidak dapat melewati langkah ini. PIN baru harus berbeda dari PIN default.
      </div>

      <div id="changePinMessage" class="message" style="display:none;"></div>

      <div class="form-group" style="text-align:center;">
        <label style="text-align:center;">PIN Baru (6 digit)</label>
        <div class="pin-input-container" id="newPinInputs">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input new-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
        </div>
      </div>

      <div class="form-group" style="text-align:center;">
        <label style="text-align:center;">Konfirmasi PIN Baru</label>
        <div class="pin-input-container" id="confirmPinInputs">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
          <input type="password" class="pin-input confirm-pin" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off">
        </div>
      </div>

      <button class="btn btn-primary" id="savePinBtn" onclick="saveNewPin()">
        Simpan PIN Baru
      </button>
    </div>
  </div>

  <script>
    let currentPhone = '';
    let currentSession = '';
    let userName = '';

    // Check if already logged in
    const existingSession = localStorage.getItem('goldmonitor_session');
    if (existingSession) {
      fetch('/api/verify-session?session=' + existingSession)
        .then(r => r.json())
        .then(data => {
          if (data.valid) {
            // Check if PIN change required
            fetch('/api/check-pin-status?session=' + existingSession)
              .then(r => r.json())
              .then(pinData => {
                if (pinData.requirePinChange) {
                  currentSession = existingSession;
                  document.getElementById('changePinModal').classList.add('show');
                  setupPinInputs(document.querySelectorAll('.new-pin'));
                  setupPinInputs(document.querySelectorAll('.confirm-pin'));
                } else {
                  window.location.replace('/monitoring');
                }
              });
          } else {
            localStorage.removeItem('goldmonitor_session');
          }
        })
        .catch(() => {});
    }

    function showMessage(text, type, elementId = 'message') {
      const msg = document.getElementById(elementId);
      msg.textContent = text;
      msg.className = 'message ' + type;
      msg.style.display = 'block';
    }

    function hideMessage(elementId = 'message') {
      const msg = document.getElementById(elementId);
      msg.className = 'message';
      msg.style.display = 'none';
    }

    function setLoading(btn, loading, text = 'Memproses...') {
      if (loading) {
        btn.disabled = true;
        btn.dataset.originalText = btn.textContent;
        btn.innerHTML = '<span class="loading"></span>' + text;
      } else {
        btn.disabled = false;
        btn.textContent = btn.dataset.originalText || 'Submit';
      }
    }

    // Setup PIN input auto-focus
    function setupPinInputs(inputs) {
      inputs.forEach((input, index) => {
        input.addEventListener('input', (e) => {
          const value = e.target.value;
          if (value && index < inputs.length - 1) {
            inputs[index + 1].focus();
          }
        });
        input.addEventListener('keydown', (e) => {
          if (e.key === 'Backspace' && !e.target.value && index > 0) {
            inputs[index - 1].focus();
          }
        });
        input.addEventListener('paste', (e) => {
          e.preventDefault();
          const paste = (e.clipboardData || window.clipboardData).getData('text');
          const digits = paste.replace(/\\D/g, '').split('').slice(0, 6);
          digits.forEach((digit, i) => {
            if (inputs[i]) inputs[i].value = digit;
          });
          if (digits.length > 0) {
            inputs[Math.min(digits.length, inputs.length - 1)].focus();
          }
        });
      });
    }

    // Get PIN value from inputs
    function getPinValue(inputs) {
      return Array.from(inputs).map(i => i.value).join('');
    }

    // Clear PIN inputs
    function clearPinInputs(inputs) {
      inputs.forEach(i => i.value = '');
      if (inputs[0]) inputs[0].focus();
    }

    // Step 1: Check if user exists
    async function checkUser() {
      const phoneInput = document.getElementById('phoneInput');
      let phone = phoneInput.value.replace(/\\D/g, '');

      if (phone.startsWith('62')) phone = phone.substring(2);
      if (phone.startsWith('0')) phone = phone.substring(1);

      if (!phone || phone.length < 9) {
        showMessage('Masukkan nomor HP yang valid', 'error');
        return;
      }

      currentPhone = phone;
      const btn = document.getElementById('checkBtn');
      setLoading(btn, true, 'Memeriksa...');
      hideMessage();

      try {
        const res = await fetch('/api/check-user', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ phone })
        });

        const data = await res.json();

        if (data.success) {
          userName = data.user.name;
          document.getElementById('userName').textContent = data.user.name;
          document.getElementById('userPhone').textContent = '+62' + phone;

          // Show PIN form
          document.getElementById('phoneForm').style.display = 'none';
          document.getElementById('pinForm').style.display = 'block';
          document.getElementById('step1').classList.remove('active');
          document.getElementById('step1').classList.add('completed');
          document.getElementById('step2').classList.add('active');

          // Setup PIN inputs
          const pinInputs = document.querySelectorAll('#pinForm .pin-input');
          setupPinInputs(pinInputs);
          pinInputs[0].focus();
        } else {
          showMessage(data.error || 'Gagal memeriksa nomor', 'error');
        }
      } catch (e) {
        showMessage('Terjadi kesalahan. Coba lagi.', 'error');
      }

      setLoading(btn, false);
      btn.textContent = 'Masuk ke Akun';
    }

    // Back to phone input
    function backToPhone() {
      document.getElementById('phoneForm').style.display = 'block';
      document.getElementById('pinForm').style.display = 'none';
      document.getElementById('step1').classList.add('active');
      document.getElementById('step1').classList.remove('completed');
      document.getElementById('step2').classList.remove('active');
      hideMessage();
      clearPinInputs(document.querySelectorAll('#pinForm .pin-input'));
    }

    // Step 2: Submit login with PIN
    async function submitLogin() {
      const pinInputs = document.querySelectorAll('#pinForm .pin-input');
      const pin = getPinValue(pinInputs);

      if (pin.length !== 6) {
        showMessage('Masukkan PIN 6 digit', 'error');
        return;
      }

      const btn = document.getElementById('loginBtn');
      setLoading(btn, true, 'Masuk...');
      hideMessage();

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ phone: currentPhone, pin })
        });

        const data = await res.json();

        if (data.success) {
          localStorage.setItem('goldmonitor_session', data.sessionId);
          currentSession = data.sessionId;

          if (data.requirePinChange) {
            // Show PIN change modal
            document.getElementById('changePinModal').classList.add('show');
            setupPinInputs(document.querySelectorAll('.new-pin'));
            setupPinInputs(document.querySelectorAll('.confirm-pin'));
            document.querySelector('.new-pin').focus();
          } else {
            showMessage('Login berhasil! Mengalihkan...', 'success');
            setTimeout(() => {
              window.location.replace('/monitoring');
            }, 500);
          }
        } else {
          showMessage(data.error || 'Login gagal', 'error');
          clearPinInputs(pinInputs);
        }
      } catch (e) {
        showMessage('Terjadi kesalahan. Coba lagi.', 'error');
      }

      setLoading(btn, false);
      btn.textContent = 'Masuk';
    }

    // Save new PIN
    async function saveNewPin() {
      const newPinInputs = document.querySelectorAll('.new-pin');
      const confirmPinInputs = document.querySelectorAll('.confirm-pin');
      const newPin = getPinValue(newPinInputs);
      const confirmPin = getPinValue(confirmPinInputs);

      hideMessage('changePinMessage');

      if (newPin.length !== 6) {
        showMessage('PIN baru harus 6 digit', 'error', 'changePinMessage');
        return;
      }

      if (newPin !== confirmPin) {
        showMessage('Konfirmasi PIN tidak cocok', 'error', 'changePinMessage');
        clearPinInputs(confirmPinInputs);
        return;
      }

      // Check if new PIN is same as default (000000)
      if (newPin === '000000') {
        showMessage('PIN baru tidak boleh sama dengan PIN default (000000)', 'error', 'changePinMessage');
        clearPinInputs(newPinInputs);
        clearPinInputs(confirmPinInputs);
        return;
      }

      const btn = document.getElementById('savePinBtn');
      setLoading(btn, true, 'Menyimpan...');

      try {
        const res = await fetch('/api/change-pin', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ session: currentSession, newPin })
        });

        const data = await res.json();

        if (data.success) {
          showMessage('PIN berhasil diubah! Mengalihkan...', 'success', 'changePinMessage');
          setTimeout(() => {
            window.location.replace('/monitoring');
          }, 1000);
        } else {
          showMessage(data.error || 'Gagal mengubah PIN', 'error', 'changePinMessage');
        }
      } catch (e) {
        showMessage('Terjadi kesalahan. Coba lagi.', 'error', 'changePinMessage');
      }

      setLoading(btn, false);
      btn.textContent = 'Simpan PIN Baru';
    }

    // Enter key handlers
    document.getElementById('phoneInput').addEventListener('keypress', (e) => {
      if (e.key === 'Enter') checkUser();
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

// ==================== LOGIN VIA LINK ====================
app.get('/auth/:token', async (req, res) => {
  const { token } = req.params

  try {
    // Get token data from Redis
    const tokenData = await redis.hget(REDIS_KEYS.LOGIN_TOKENS, token)
    if (!tokenData) {
      return res.send(getLoginErrorPage('Link login tidak valid atau sudah kadaluarsa.'))
    }

    const data = typeof tokenData === 'string' ? JSON.parse(tokenData) : tokenData

    // Check expiry (5 minutes)
    if (Date.now() > data.expires) {
      await redis.hdel(REDIS_KEYS.LOGIN_TOKENS, token)
      return res.send(getLoginErrorPage('Link login sudah kadaluarsa. Silakan minta link baru.'))
    }

    const phone = data.phone

    // Check if user is blocked
    const blocked = await redis.hget(REDIS_KEYS.BLOCKED_USERS, phone)
    if (blocked) {
      return res.send(getLoginErrorPage('Akun Anda diblokir. Hubungi admin untuk membuka blokir.'))
    }

    // Check if user is valid
    const check = await isUserValid(phone)
    if (!check.valid) {
      if (check.reason === 'expired') {
        return res.send(getLoginErrorPage('Akun sudah expired. Hubungi admin untuk perpanjang.'))
      }
      return res.send(getLoginErrorPage('Akun tidak ditemukan atau tidak valid.'))
    }

    // Check existing sessions for this user (max 2 devices)
    const allSessions = await redis.hgetall(REDIS_KEYS.SESSIONS) || {}
    const userSessions = []
    for (const [sessId, sessPhone] of Object.entries(allSessions)) {
      if (sessPhone === phone) {
        userSessions.push(sessId)
      }
    }

    // If already 2 sessions, remove the oldest one
    if (userSessions.length >= 2) {
      await redis.hdel(REDIS_KEYS.SESSIONS, userSessions[0])
      pushLog('Auth | User +62' + phone + ' exceeded 2 devices, oldest session removed')
    }

    // Create new session
    const sessionId = generateSessionId()
    await redis.hset(REDIS_KEYS.SESSIONS, { [sessionId]: phone })

    // Delete used token
    await redis.hdel(REDIS_KEYS.LOGIN_TOKENS, token)

    pushLog('Auth | User +62' + phone + ' logged in via link')

    // Return success page that saves session and redirects
    const userName = check.user?.name || 'User'
    res.send('<!DOCTYPE html>' +
'<html>' +
'<head>' +
'  <meta charset="UTF-8">' +
'  <meta name="viewport" content="width=device-width, initial-scale=1.0">' +
'  <meta name="theme-color" content="#0f1419">' +
'  <link rel="icon" href="/icon.png">' +
'  <title>Login Berhasil</title>' +
'  <style>' +
'    body {' +
'      font-family: "Segoe UI", sans-serif;' +
'      background: linear-gradient(135deg, #0f1419, #1a1f26);' +
'      min-height: 100vh;' +
'      display: flex;' +
'      align-items: center;' +
'      justify-content: center;' +
'      margin: 0;' +
'      color: #e7e9ea;' +
'    }' +
'    .card {' +
'      background: rgba(26, 31, 38, 0.95);' +
'      border-radius: 20px;' +
'      padding: 40px;' +
'      text-align: center;' +
'      border: 1px solid #2f3640;' +
'      max-width: 400px;' +
'    }' +
'    .success-icon { font-size: 60px; margin-bottom: 20px; }' +
'    h1 { color: #00ff88; margin-bottom: 10px; }' +
'    p { color: #71767b; }' +
'    .loading {' +
'      display: inline-block;' +
'      width: 30px;' +
'      height: 30px;' +
'      border: 3px solid #2f3640;' +
'      border-radius: 50%;' +
'      border-top-color: #f7931a;' +
'      animation: spin 1s linear infinite;' +
'      margin-top: 20px;' +
'    }' +
'    @keyframes spin { to { transform: rotate(360deg); } }' +
'  </style>' +
'</head>' +
'<body>' +
'  <div class="card">' +
'    <div class="success-icon">✅</div>' +
'    <h1>Login Berhasil!</h1>' +
'    <p>Selamat datang, ' + userName + '</p>' +
'    <p style="margin-top:10px;">Mengalihkan ke monitoring...</p>' +
'    <div class="loading"></div>' +
'  </div>' +
'  <script>' +
'    localStorage.setItem("goldmonitor_session", "' + sessionId + '");' +
'    setTimeout(function() {' +
'      window.location.replace("/monitoring");' +
'    }, 1500);' +
'  </script>' +
'</body>' +
'</html>')

  } catch (e) {
    pushLog('Auth | Login link error: ' + e.message)
    res.send(getLoginErrorPage('Terjadi kesalahan. Silakan coba lagi.'))
  }
})

// Helper: Login error page
function getLoginErrorPage(message) {
  return '<!DOCTYPE html>' +
'<html>' +
'<head>' +
'  <meta charset="UTF-8">' +
'  <meta name="viewport" content="width=device-width, initial-scale=1.0">' +
'  <meta name="theme-color" content="#0f1419">' +
'  <link rel="icon" href="/icon.png">' +
'  <title>Login Gagal</title>' +
'  <style>' +
'    body {' +
'      font-family: "Segoe UI", sans-serif;' +
'      background: linear-gradient(135deg, #0f1419, #1a1f26);' +
'      min-height: 100vh;' +
'      display: flex;' +
'      align-items: center;' +
'      justify-content: center;' +
'      margin: 0;' +
'      color: #e7e9ea;' +
'    }' +
'    .card {' +
'      background: rgba(26, 31, 38, 0.95);' +
'      border-radius: 20px;' +
'      padding: 40px;' +
'      text-align: center;' +
'      border: 1px solid #2f3640;' +
'      max-width: 400px;' +
'    }' +
'    .error-icon { font-size: 60px; margin-bottom: 20px; }' +
'    h1 { color: #ff6b6b; margin-bottom: 10px; }' +
'    p { color: #71767b; margin-bottom: 20px; }' +
'    a {' +
'      display: inline-block;' +
'      background: linear-gradient(135deg, #f7931a, #ff6b00);' +
'      color: white;' +
'      padding: 12px 30px;' +
'      border-radius: 10px;' +
'      text-decoration: none;' +
'      font-weight: bold;' +
'    }' +
'  </style>' +
'</head>' +
'<body>' +
'  <div class="card">' +
'    <div class="error-icon">❌</div>' +
'    <h1>Login Gagal</h1>' +
'    <p>' + message + '</p>' +
'    <a href="/login">Coba Lagi</a>' +
'  </div>' +
'</body>' +
'</html>'
}

// Redirect /install to /login
app.get('/install', (_req, res) => {
  res.redirect('/login');
})

// ==================== ADMIN PANEL - USER MANAGEMENT ====================
app.get('/admin/users', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate')
  const authScript = getAuthCheckScript('/admin/users')
  const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">
  <title>Admin - Kelola User</title>
${authScript}
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: linear-gradient(180deg, #0a0e13 0%, #0f1419 100%);
      min-height: 100vh;
      padding: 20px;
      color: #e7e9ea;
    }
    .container { max-width: 1100px; margin: 0 auto; }

    /* Header Modern */
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding: 18px 24px;
      background: rgba(20, 26, 34, 0.9);
      backdrop-filter: blur(20px);
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.06);
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .header h1 { color: #ffffff; font-size: 1.4em; font-weight: 700; letter-spacing: -0.02em; display: flex; align-items: center; gap: 10px; }
    .header h1 svg { width: 24px; height: 24px; color: #f7931a; }
    .header-actions { display: flex; gap: 10px; }
    .header-actions a {
      padding: 10px 16px;
      background: rgba(255,255,255,0.06);
      color: #e7e9ea;
      text-decoration: none;
      border-radius: 10px;
      font-size: 0.85em;
      font-weight: 500;
      border: 1px solid rgba(255,255,255,0.08);
      transition: all 0.2s ease;
    }
    .header-actions a:hover { background: rgba(247,147,26,0.15); border-color: rgba(247,147,26,0.3); color: #f7931a; }

    /* Stats Cards */
    .stats-row {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
      margin-bottom: 24px;
    }
    .stat-card {
      background: rgba(20, 26, 34, 0.85);
      backdrop-filter: blur(10px);
      padding: 20px 16px;
      border-radius: 14px;
      text-align: center;
      border: 1px solid rgba(255,255,255,0.06);
      transition: all 0.2s;
    }
    .stat-card:hover { border-color: rgba(247,147,26,0.2); transform: translateY(-2px); }
    .stat-value { font-size: 1.8em; font-weight: 700; color: #f7931a; font-family: 'JetBrains Mono', monospace; }
    .stat-label { color: #8b949e; font-size: 0.8em; margin-top: 4px; font-weight: 500; }

    /* Cards */
    .card {
      background: rgba(20, 26, 34, 0.85);
      backdrop-filter: blur(20px);
      border-radius: 16px;
      padding: 22px;
      margin-bottom: 18px;
      border: 1px solid rgba(255,255,255,0.06);
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    }
    .card h2 {
      color: #ffffff;
      font-size: 1em;
      font-weight: 600;
      margin-bottom: 16px;
      padding-bottom: 12px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
      letter-spacing: -0.01em;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .card h2 svg { width: 18px; height: 18px; color: #f7931a; }

    /* Section Tabs */
    .section-tabs {
      display: flex;
      gap: 8px;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }
    .section-tab {
      padding: 10px 18px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 10px;
      color: #8b949e;
      font-size: 0.85em;
      font-weight: 500;
      cursor: pointer;
      transition: all 0.2s;
    }
    .section-tab:hover { background: rgba(255,255,255,0.08); color: #e7e9ea; }
    .section-tab.active { background: rgba(247,147,26,0.15); border-color: rgba(247,147,26,0.3); color: #f7931a; }

    /* Section Content */
    .section-content { display: none; }
    .section-content.active { display: block; }

    /* Forms */
    .form-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 16px;
    }
    .form-group { margin-bottom: 14px; }
    .form-group label {
      display: block;
      margin-bottom: 8px;
      color: #8b949e;
      font-size: 0.82em;
      font-weight: 500;
    }
    .form-group input, .form-group select, .form-group textarea {
      width: 100%;
      padding: 11px 14px;
      border: 1px solid rgba(255,255,255,0.1);
      border-radius: 10px;
      background: rgba(15, 20, 25, 0.9);
      color: #e7e9ea;
      font-size: 0.9em;
      font-family: inherit;
      transition: all 0.2s ease;
    }
    .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
      outline: none;
      border-color: #f7931a;
      box-shadow: 0 0 0 3px rgba(247,147,26,0.12);
    }
    .form-group textarea { resize: vertical; min-height: 100px; }

    /* Buttons */
    .btn {
      padding: 10px 18px;
      border: none;
      border-radius: 10px;
      font-size: 0.88em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      font-family: inherit;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .btn-primary {
      background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%);
      color: white;
      box-shadow: 0 4px 12px rgba(247,147,26,0.25);
    }
    .btn-primary:hover { transform: translateY(-1px); box-shadow: 0 6px 16px rgba(247,147,26,0.35); }
    .btn-secondary { background: rgba(255,255,255,0.08); color: #e7e9ea; border: 1px solid rgba(255,255,255,0.1); }
    .btn-secondary:hover { background: rgba(255,255,255,0.12); }
    .btn-success { background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); color: white; }
    .btn-success:hover { background: linear-gradient(135deg, #4ade80 0%, #22c55e 100%); }
    .btn-danger { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color: white; }
    .btn-danger:hover { background: linear-gradient(135deg, #f87171 0%, #ef4444 100%); }
    .btn-warning { background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); color: white; }
    .btn-sm { padding: 6px 12px; font-size: 0.78em; }
    .btn-xs { padding: 4px 8px; font-size: 0.72em; border-radius: 6px; }

    /* Action Buttons Group */
    .action-btns {
      display: flex;
      gap: 4px;
      flex-wrap: wrap;
    }
    .action-btn {
      padding: 5px 10px;
      border-radius: 6px;
      font-size: 0.72em;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.15s;
    }
    .action-btn.edit { background: rgba(59,130,246,0.15); color: #60a5fa; }
    .action-btn.edit:hover { background: rgba(59,130,246,0.25); }
    .action-btn.push { background: rgba(168,85,247,0.15); color: #c084fc; }
    .action-btn.push:hover { background: rgba(168,85,247,0.25); }
    .action-btn.pin { background: rgba(247,147,26,0.15); color: #f7931a; }
    .action-btn.pin:hover { background: rgba(247,147,26,0.25); }
    .action-btn.block { background: rgba(239,68,68,0.15); color: #f87171; }
    .action-btn.block:hover { background: rgba(239,68,68,0.25); }
    .action-btn.unblock { background: rgba(34,197,94,0.15); color: #4ade80; }
    .action-btn.unblock:hover { background: rgba(34,197,94,0.25); }
    .action-btn.delete { background: rgba(239,68,68,0.2); color: #f87171; }
    .action-btn.delete:hover { background: rgba(239,68,68,0.35); }
    .action-btn.kick { background: rgba(249,115,22,0.15); color: #fb923c; }
    .action-btn.kick:hover { background: rgba(249,115,22,0.25); }

    /* User Table */
    .user-table-wrapper {
      overflow-x: auto;
      margin: 0 -10px;
      padding: 0 10px;
    }
    .user-table {
      width: 100%;
      border-collapse: collapse;
      min-width: 700px;
    }
    .user-table th, .user-table td {
      padding: 12px 10px;
      text-align: left;
      border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .user-table th {
      color: #8b949e;
      font-size: 0.72em;
      text-transform: uppercase;
      font-weight: 600;
      letter-spacing: 0.5px;
      background: rgba(0,0,0,0.2);
      position: sticky;
      top: 0;
    }
    .user-table tr:hover { background: rgba(247,147,26,0.04); }
    .user-table td { font-size: 0.85em; }
    .user-table td.phone { font-family: 'JetBrains Mono', monospace; font-size: 0.8em; }

    /* Status Badges */
    .status-badge {
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 0.72em;
      font-weight: 600;
      font-family: 'Inter', sans-serif;
      display: inline-block;
    }
    .status-active { background: rgba(74,222,128,0.15); color: #4ade80; }
    .status-expired { background: rgba(248,113,113,0.15); color: #f87171; }
    .status-lifetime { background: rgba(247,147,26,0.15); color: #f7931a; }
    .status-blocked { background: rgba(248,113,113,0.25); color: #f87171; }

    /* Push Badge */
    .push-badge {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
    }
    .push-yes { background: #4ade80; box-shadow: 0 0 6px rgba(74,222,128,0.5); }
    .push-no { background: #6b7280; }

    /* PIN Badge */
    .pin-badge {
      padding: 3px 8px;
      border-radius: 12px;
      font-size: 0.68em;
      font-weight: 600;
    }
    .pin-changed { background: rgba(74,222,128,0.15); color: #4ade80; }
    .pin-default { background: rgba(251,191,36,0.15); color: #fbbf24; }

    /* Result Messages */
    .result-msg {
      padding: 12px 16px;
      border-radius: 10px;
      margin-bottom: 14px;
      display: none;
      font-weight: 500;
      font-size: 0.88em;
    }
    .result-msg.success { display: block; background: rgba(74,222,128,0.1); border: 1px solid rgba(74,222,128,0.25); color: #4ade80; }
    .result-msg.error { display: block; background: rgba(248,113,113,0.1); border: 1px solid rgba(248,113,113,0.25); color: #f87171; }

    /* Modal */
    .modal {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.8);
      backdrop-filter: blur(4px);
      align-items: center;
      justify-content: center;
      z-index: 1000;
    }
    .modal.show { display: flex; }
    .modal-content {
      background: rgba(20, 26, 34, 0.98);
      backdrop-filter: blur(20px);
      padding: 24px;
      border-radius: 16px;
      width: 90%;
      max-width: 400px;
      border: 1px solid rgba(255,255,255,0.08);
      box-shadow: 0 20px 50px rgba(0,0,0,0.4);
    }
    .modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
    }
    .modal-header h3 { color: #ffffff; font-weight: 600; font-size: 1.05em; }
    .modal-close {
      background: rgba(255,255,255,0.08);
      border: none;
      color: #8b949e;
      font-size: 1.2em;
      cursor: pointer;
      padding: 6px 10px;
      border-radius: 8px;
      transition: all 0.2s;
    }
    .modal-close:hover { background: rgba(255,255,255,0.15); color: #fff; }

    /* Empty State */
    .empty-state {
      text-align: center;
      padding: 40px;
      color: #6b7280;
      font-size: 0.9em;
    }

    /* Warning Box */
    .warning-box {
      padding: 12px 14px;
      background: rgba(251,191,36,0.08);
      border: 1px solid rgba(251,191,36,0.2);
      border-radius: 10px;
      margin-top: 14px;
    }
    .warning-box p { color: #fbbf24; font-size: 0.82em; margin: 0; line-height: 1.5; }

    /* Buttons Row */
    .btns-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }

    /* Responsive */
    @media (max-width: 900px) {
      .stats-row { grid-template-columns: repeat(2, 1fr); }
      .form-row { grid-template-columns: 1fr; }
    }
    @media (max-width: 768px) {
      body { padding: 12px; }
      .header { flex-direction: column; gap: 12px; text-align: center; padding: 14px 18px; }
      .header h1 { font-size: 1.2em; }
      .header-actions { justify-content: center; }
      .stats-row { gap: 10px; }
      .stat-card { padding: 16px 12px; }
      .stat-value { font-size: 1.5em; }
      .section-tabs { gap: 6px; }
      .section-tab { padding: 8px 14px; font-size: 0.8em; }
      .card { padding: 16px; }
    }
    @media (max-width: 500px) {
      body { padding: 8px; }
      .stats-row { grid-template-columns: repeat(2, 1fr); gap: 8px; }
      .stat-card { padding: 12px 8px; }
      .stat-value { font-size: 1.3em; }
      .stat-label { font-size: 0.7em; }
      .header-actions a { padding: 8px 12px; font-size: 0.78em; }
      .section-tab { padding: 7px 12px; font-size: 0.75em; }
      .btn { padding: 8px 14px; font-size: 0.82em; }
      .action-btn { padding: 4px 7px; font-size: 0.68em; }
    }

    /* Professional Modal System */
    .pro-modal-overlay {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.85);
      backdrop-filter: blur(8px);
      align-items: center;
      justify-content: center;
      z-index: 9999;
      animation: fadeIn 0.2s ease;
    }
    .pro-modal-overlay.show { display: flex; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
    @keyframes slideUp { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
    .pro-modal-box {
      background: linear-gradient(180deg, rgba(25, 32, 42, 0.98) 0%, rgba(18, 24, 32, 0.98) 100%);
      border-radius: 16px;
      width: 90%;
      max-width: 380px;
      border: 1px solid rgba(255,255,255,0.1);
      box-shadow: 0 25px 60px rgba(0,0,0,0.5);
      animation: slideUp 0.25s ease;
      overflow: hidden;
    }
    .pro-modal-icon {
      width: 56px;
      height: 56px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 24px auto 16px;
    }
    .pro-modal-icon.info { background: rgba(59,130,246,0.15); color: #60a5fa; }
    .pro-modal-icon.success { background: rgba(34,197,94,0.15); color: #4ade80; }
    .pro-modal-icon.warning { background: rgba(251,191,36,0.15); color: #fbbf24; }
    .pro-modal-icon.danger { background: rgba(239,68,68,0.15); color: #f87171; }
    .pro-modal-icon svg { width: 28px; height: 28px; }
    .pro-modal-content { padding: 0 24px 24px; text-align: center; }
    .pro-modal-title { color: #fff; font-size: 1.1em; font-weight: 600; margin-bottom: 8px; }
    .pro-modal-message { color: #9ca3af; font-size: 0.9em; line-height: 1.5; }
    .pro-modal-buttons { display: flex; gap: 10px; margin-top: 20px; justify-content: center; }
    .pro-modal-btn {
      padding: 10px 20px;
      border-radius: 10px;
      font-size: 0.88em;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.2s;
      min-width: 100px;
    }
    .pro-modal-btn.cancel { background: rgba(255,255,255,0.08); color: #e7e9ea; }
    .pro-modal-btn.cancel:hover { background: rgba(255,255,255,0.15); }
    .pro-modal-btn.confirm { background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%); color: white; }
    .pro-modal-btn.confirm:hover { transform: translateY(-1px); }
    .pro-modal-btn.danger { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color: white; }
    .pro-modal-btn.danger:hover { transform: translateY(-1px); }
  </style>
</head>
<body>
  <!-- Professional Modal -->
  <div class="pro-modal-overlay" id="proModal">
    <div class="pro-modal-box">
      <div class="pro-modal-icon info" id="proModalIcon">
        <svg id="proModalSvg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"></svg>
      </div>
      <div class="pro-modal-content">
        <div class="pro-modal-title" id="proModalTitle">Title</div>
        <div class="pro-modal-message" id="proModalMessage">Message</div>
        <div class="pro-modal-buttons" id="proModalButtons"></div>
      </div>
    </div>
  </div>
  <div class="container">
    <div id="mainContent">
      <!-- Header -->
      <div class="header">
        <h1>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/>
            <circle cx="9" cy="7" r="4"/>
            <path d="M22 21v-2a4 4 0 0 0-3-3.87"/>
            <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
          </svg>
          Kelola User
        </h1>
        <div class="header-actions">
          <a href="/admin/monitoring">Notifikasi</a>
          <a href="/monitoring" target="_blank">Monitoring</a>
        </div>
      </div>

      <!-- Stats -->
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
        <div class="stat-card">
          <div class="stat-value" id="pinChangedUsers">0</div>
          <div class="stat-label">PIN Changed</div>
        </div>
      </div>

      <!-- Section Tabs -->
      <div class="section-tabs">
        <div class="section-tab active" data-section="users">Daftar User</div>
        <div class="section-tab" data-section="online">Online <span id="onlineBadge" style="background:#22c55e;color:#fff;padding:1px 6px;border-radius:8px;font-size:0.75em;margin-left:4px;">0</span></div>
        <div class="section-tab" data-section="add">Tambah User</div>
        <div class="section-tab" data-section="pending">Pending <span id="pendingBadge" style="background:#f7931a;color:#000;padding:1px 6px;border-radius:8px;font-size:0.75em;margin-left:4px;">0</span></div>
        <div class="section-tab" data-section="whatsapp">WhatsApp</div>
        <div class="section-tab" data-section="settings">Pengaturan</div>
      </div>

      <!-- Section: Daftar User -->
      <div class="section-content active" id="section-users">
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
              <circle cx="9" cy="7" r="4"/>
              <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
              <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
            </svg>
            Daftar User
          </h2>
          <div class="user-table-wrapper">
            <table class="user-table">
              <thead>
                <tr>
                  <th>No WA</th>
                  <th>Nama</th>
                  <th>Status</th>
                  <th>Push</th>
                  <th>PIN</th>
                  <th>Expired</th>
                  <th>Aksi</th>
                </tr>
              </thead>
              <tbody id="userList">
                <tr><td colspan="7" class="empty-state">Memuat data...</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- Section: Online Users -->
      <div class="section-content" id="section-online">
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"/>
              <circle cx="12" cy="12" r="3" fill="#22c55e"/>
            </svg>
            User Online
            <span id="onlineCount" style="background:#22c55e;color:#fff;padding:2px 10px;border-radius:12px;font-size:0.75em;margin-left:8px;">0</span>
          </h2>
          <p style="color:#6b7280;font-size:0.82em;margin-bottom:14px;">Daftar user yang sedang membuka halaman monitoring secara realtime.</p>
          <div class="user-table-wrapper">
            <table class="user-table">
              <thead>
                <tr>
                  <th style="width:40px;">#</th>
                  <th>Nama</th>
                  <th>No WA</th>
                  <th>Waktu Terhubung</th>
                </tr>
              </thead>
              <tbody id="onlineUsersList">
                <tr><td colspan="4" class="empty-state">Tidak ada user online</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- Section: Tambah User -->
      <div class="section-content" id="section-add">
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/>
              <circle cx="9" cy="7" r="4"/>
              <line x1="19" y1="8" x2="19" y2="14"/>
              <line x1="22" y1="11" x2="16" y2="11"/>
            </svg>
            Tambah User Manual
          </h2>
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
              <label>Tanggal Expired</label>
              <input type="date" id="newExpiredDate">
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
            <button class="btn btn-primary" onclick="addUser()">Tambah User</button>
            <small style="color:#6b7280;">Kosongkan tanggal untuk lifetime</small>
          </div>
        </div>

        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
              <polyline points="17 8 12 3 7 8"/>
              <line x1="12" y1="3" x2="12" y2="15"/>
            </svg>
            Import Bulk User
          </h2>
          <div class="result-msg" id="bulkResult"></div>
          <div class="form-group">
            <label>Daftar Nomor (satu per baris atau pisahkan dengan koma)</label>
            <textarea id="bulkPhones" placeholder="08123456789&#10;08234567890&#10;08345678901"></textarea>
          </div>
          <button class="btn btn-primary" onclick="bulkImport()">Import Semua</button>
        </div>
      </div>

      <!-- Section: Pending -->
      <div class="section-content" id="section-pending">
        <div class="card" style="border-color:rgba(247,147,26,0.3);">
          <h2 style="color:#f7931a;">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"/>
              <polyline points="12 6 12 12 16 14"/>
            </svg>
            Pending Registrasi <span id="pendingCount" style="background:#f7931a;color:#000;padding:2px 8px;border-radius:10px;font-size:0.7em;margin-left:5px;">0</span>
          </h2>
          <div class="user-table-wrapper">
            <table class="user-table">
              <thead>
                <tr>
                  <th>Waktu</th>
                  <th>Nama</th>
                  <th>No WA</th>
                  <th>Aksi</th>
                </tr>
              </thead>
              <tbody id="pendingList">
                <tr><td colspan="4" class="empty-state">Tidak ada pendaftaran baru</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- Section: WhatsApp -->
      <div class="section-content" id="section-whatsapp">
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/>
            </svg>
            Sinkronisasi Grup WhatsApp
          </h2>
          <p style="color:#6b7280;font-size:0.85em;margin-bottom:14px;">Member grup yang dipilih akan otomatis terdaftar dan bisa login ke website.</p>
          <div class="result-msg" id="syncResult"></div>
          <div class="form-row" style="align-items:flex-end;">
            <div class="form-group" style="flex:2;">
              <label>Pilih Grup WhatsApp</label>
              <select id="waGroupSelect">
                <option value="">-- Pilih Grup --</option>
              </select>
            </div>
            <div class="form-group" style="flex:1;">
              <button class="btn btn-primary" onclick="setWaGroup()" style="width:100%;">Set Grup</button>
            </div>
          </div>
          <div id="currentGroup" style="margin-top:8px;font-size:0.82em;color:#6b7280;"></div>
          <div class="btns-row">
            <button class="btn btn-secondary" onclick="loadWaGroups()">Refresh Grup</button>
            <button class="btn btn-danger btn-sm" onclick="clearInvalidUsers()">Hapus Invalid</button>
            <button class="btn btn-sm" style="background:#7f1d1d;color:white;" onclick="clearAllUsers()">Hapus Semua</button>
            <button class="btn btn-sm" style="background:#f59e0b;color:#000;" onclick="forceLogoutAll()">Force Logout Semua</button>
          </div>
          <div class="warning-box">
            <p><strong>Catatan:</strong> WhatsApp menggunakan format LID (privacy) sehingga nomor telepon member tidak bisa diakses otomatis. User harus mendaftar sendiri via OTP atau ditambahkan manual oleh admin.</p>
          </div>
        </div>
      </div>

      <!-- Section: Pengaturan -->
      <div class="section-content" id="section-settings">
        <!-- Sound Notifikasi -->
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/>
              <path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"/>
            </svg>
            Sound Notifikasi
          </h2>
          <p style="color:#6b7280;font-size:0.82em;margin-bottom:16px;">Upload file audio atau masukkan URL. Max 500KB per file.</p>
          <div class="result-msg" id="soundResult"></div>

          <!-- Sound Naik -->
          <div style="background:rgba(74,222,128,0.05);padding:16px;border-radius:12px;border:1px solid rgba(74,222,128,0.15);margin-bottom:14px;">
            <label style="color:#4ade80;font-weight:600;display:block;margin-bottom:12px;font-size:0.9em;">Sound Harga Naik</label>
            <div class="form-group" style="margin-bottom:10px;">
              <label>Upload File Audio</label>
              <input type="file" id="soundUpFile" accept="audio/*" onchange="handleSoundUpload('up')">
            </div>
            <div class="form-group" style="margin-bottom:10px;">
              <label>Atau Masukkan URL</label>
              <input type="text" id="soundUpUrl" placeholder="https://example.com/naik.mp3">
            </div>
            <div id="soundUpPreview" style="margin-top:10px;display:none;">
              <audio id="soundUpAudio" controls style="width:100%;height:36px;"></audio>
            </div>
            <button class="btn btn-sm" style="margin-top:10px;background:rgba(74,222,128,0.15);color:#4ade80;border:1px solid rgba(74,222,128,0.25);" onclick="testSound('up')">Test Sound Naik</button>
          </div>

          <!-- Sound Turun -->
          <div style="background:rgba(248,113,113,0.05);padding:16px;border-radius:12px;border:1px solid rgba(248,113,113,0.15);margin-bottom:16px;">
            <label style="color:#f87171;font-weight:600;display:block;margin-bottom:12px;font-size:0.9em;">Sound Harga Turun</label>
            <div class="form-group" style="margin-bottom:10px;">
              <label>Upload File Audio</label>
              <input type="file" id="soundDownFile" accept="audio/*" onchange="handleSoundUpload('down')">
            </div>
            <div class="form-group" style="margin-bottom:10px;">
              <label>Atau Masukkan URL</label>
              <input type="text" id="soundDownUrl" placeholder="https://example.com/turun.mp3">
            </div>
            <div id="soundDownPreview" style="margin-top:10px;display:none;">
              <audio id="soundDownAudio" controls style="width:100%;height:36px;"></audio>
            </div>
            <button class="btn btn-sm" style="margin-top:10px;background:rgba(248,113,113,0.15);color:#f87171;border:1px solid rgba(248,113,113,0.25);" onclick="testSound('down')">Test Sound Turun</button>
          </div>

          <div style="display:flex;gap:10px;flex-wrap:wrap;">
            <button class="btn btn-primary" onclick="saveSoundSettings()">Simpan Sound</button>
            <button class="btn btn-danger btn-sm" onclick="resetSounds()">Reset Default</button>
          </div>
        </div>

        <!-- Admin Phones -->
        <div class="card">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/>
            </svg>
            Nomor Admin untuk Notifikasi
          </h2>
          <p style="color:#6b7280;font-size:0.82em;margin-bottom:14px;">Nomor yang menerima notifikasi WhatsApp saat ada pendaftaran baru. Maksimal 2 nomor.</p>
          <div class="result-msg" id="adminPhoneResult"></div>
          <div class="form-group">
            <label>Nomor Admin 1 (Utama)</label>
            <input type="tel" id="adminPhone1" placeholder="0895701692525">
          </div>
          <div class="form-group">
            <label>Nomor Admin 2 (Opsional)</label>
            <input type="tel" id="adminPhone2" placeholder="08xxxxxxxxxx">
          </div>
          <button class="btn btn-primary" onclick="saveAdminPhones()">Simpan Nomor Admin</button>
        </div>
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
        <input type="text" id="editPhone" readonly style="opacity:0.6;">
      </div>
      <div class="form-group">
        <label>Nama</label>
        <input type="text" id="editName">
      </div>
      <div class="form-group">
        <label>Tanggal Expired</label>
        <input type="date" id="editExpiredDate">
        <small style="color:#6b7280;font-size:0.8em;">Kosongkan untuk lifetime</small>
      </div>
      <div class="form-group">
        <label>Atau Tambah Hari dari Sekarang</label>
        <input type="number" id="editAddDays" placeholder="30" min="0">
      </div>
      <button class="btn btn-primary" style="width:100%;margin-top:12px;" onclick="saveUser()">Simpan</button>
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
      <button class="btn btn-primary" style="width:100%;margin-top:12px;" onclick="sendPush()">Kirim</button>
    </div>
  </div>

  <script>
    // Admin sudah terautentikasi via /admin-login
    const adminPass = 'admin123'; // Default password untuk API calls

    // ==================== Professional Modal System ====================
    const modalIcons = {
      info: '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>',
      success: '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
      warning: '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
      danger: '<circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/>'
    };

    function showModal(options) {
      return new Promise((resolve) => {
        const modal = document.getElementById('proModal');
        const icon = document.getElementById('proModalIcon');
        const svg = document.getElementById('proModalSvg');
        const title = document.getElementById('proModalTitle');
        const message = document.getElementById('proModalMessage');
        const buttons = document.getElementById('proModalButtons');

        const type = options.type || 'info';
        icon.className = 'pro-modal-icon ' + type;
        svg.innerHTML = modalIcons[type] || modalIcons.info;
        title.textContent = options.title || '';
        message.textContent = options.message || '';

        buttons.innerHTML = '';
        if (options.showCancel !== false && options.confirmText) {
          const cancelBtn = document.createElement('button');
          cancelBtn.className = 'pro-modal-btn cancel';
          cancelBtn.textContent = options.cancelText || 'Batal';
          cancelBtn.onclick = () => { modal.classList.remove('show'); resolve(false); };
          buttons.appendChild(cancelBtn);
        }

        const confirmBtn = document.createElement('button');
        confirmBtn.className = 'pro-modal-btn ' + (type === 'danger' ? 'danger' : 'confirm');
        confirmBtn.textContent = options.confirmText || 'OK';
        confirmBtn.onclick = () => { modal.classList.remove('show'); resolve(true); };
        buttons.appendChild(confirmBtn);

        modal.classList.add('show');
      });
    }

    function showAlert(message, type = 'info') {
      return showModal({ type, title: type === 'success' ? 'Berhasil' : type === 'danger' ? 'Error' : 'Informasi', message, showCancel: false, confirmText: 'OK' });
    }

    function showConfirm(message, options = {}) {
      return showModal({ type: options.type || 'warning', title: options.title || 'Konfirmasi', message, confirmText: options.confirmText || 'Ya', cancelText: options.cancelText || 'Batal' });
    }

    // Tab Navigation
    document.querySelectorAll('.section-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.section-tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.section-content').forEach(s => s.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById('section-' + tab.dataset.section).classList.add('active');
      });
    });

    // Load data langsung saat halaman dibuka
    document.addEventListener('DOMContentLoaded', function() {
      loadUsers();
      loadPendingRegistrations();
      loadWaGroups();
      loadAdminPhones();
      loadSoundSettings();
      connectAdminSSE();
    });

    // ==================== Online Users SSE ====================
    let adminEvtSource = null;

    function connectAdminSSE() {
      if (adminEvtSource) {
        adminEvtSource.close();
      }
      adminEvtSource = new EventSource('/admin-sse');

      adminEvtSource.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          if (data.type === 'online_users') {
            updateOnlineUsers(data.users, data.count);
          }
        } catch (e) {}
      };

      adminEvtSource.onerror = function() {
        // Reconnect after 5 seconds
        setTimeout(connectAdminSSE, 5000);
      };
    }

    function updateOnlineUsers(users, count) {
      // Update badge
      document.getElementById('onlineBadge').textContent = count;
      document.getElementById('onlineCount').textContent = count;

      // Update table
      const tbody = document.getElementById('onlineUsersList');
      if (!tbody) return;

      if (!users || users.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="empty-state">Tidak ada user online</td></tr>';
        return;
      }

      let html = '';
      users.forEach((user, index) => {
        const connectedAt = new Date(user.connectedAt);
        const timeStr = connectedAt.toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        const isAnonymous = !user.phone || user.phone === 'anonymous';
        const displayName = user.name && user.name !== 'Anonymous' ? user.name : (isAnonymous ? '-' : 'Member');
        const displayPhone = isAnonymous ? '<span style="color:#6b7280;">Guest</span>' : '+' + user.phone;

        html += '<tr>' +
          '<td>' + (index + 1) + '</td>' +
          '<td>' + displayName + '</td>' +
          '<td class="phone">' + displayPhone + '</td>' +
          '<td>' + timeStr + '</td>' +
          '</tr>';
      });
      tbody.innerHTML = html;
    }

    function formatPhone(phone) {
      if (!phone) return '-';
      // Format: 628xxx -> 08xxx
      if (phone.startsWith('62')) {
        return '0' + phone.substring(2);
      }
      return phone;
    }

    // ==================== Sound Settings Functions ====================
    let currentSoundUp = '';
    let currentSoundDown = '';

    function loadSoundSettings() {
      fetch('/api/sound-settings')
        .then(r => r.json())
        .then(data => {
          if (data.success) {
            currentSoundUp = data.settings.soundUp || '';
            currentSoundDown = data.settings.soundDown || '';
            document.getElementById('soundUpUrl').value = currentSoundUp;
            document.getElementById('soundDownUrl').value = currentSoundDown;

            if (currentSoundUp) {
              document.getElementById('soundUpPreview').style.display = 'block';
              document.getElementById('soundUpAudio').src = currentSoundUp;
            }
            if (currentSoundDown) {
              document.getElementById('soundDownPreview').style.display = 'block';
              document.getElementById('soundDownAudio').src = currentSoundDown;
            }
          }
        });
    }

    function handleSoundUpload(direction) {
      const fileInput = document.getElementById(direction === 'up' ? 'soundUpFile' : 'soundDownFile');
      const urlInput = document.getElementById(direction === 'up' ? 'soundUpUrl' : 'soundDownUrl');
      const preview = document.getElementById(direction === 'up' ? 'soundUpPreview' : 'soundDownPreview');
      const audio = document.getElementById(direction === 'up' ? 'soundUpAudio' : 'soundDownAudio');
      const result = document.getElementById('soundResult');

      const file = fileInput.files[0];
      if (!file) return;

      if (file.size > 500 * 1024) {
        result.className = 'result-msg error';
        result.textContent = 'File terlalu besar! Maksimal 500KB. File Anda: ' + Math.round(file.size/1024) + 'KB';
        fileInput.value = '';
        return;
      }

      if (!file.type.startsWith('audio/')) {
        result.className = 'result-msg error';
        result.textContent = 'File harus berformat audio (MP3, WAV, OGG, dll)';
        fileInput.value = '';
        return;
      }

      const reader = new FileReader();
      reader.onload = function(e) {
        const dataUrl = e.target.result;
        urlInput.value = dataUrl;
        audio.src = dataUrl;
        preview.style.display = 'block';
        result.className = 'result-msg success';
        result.textContent = 'File "' + file.name + '" berhasil dimuat. Klik "Simpan Sound" untuk menyimpan.';
        setTimeout(() => result.className = 'result-msg', 5000);
      };
      reader.onerror = function() {
        result.className = 'result-msg error';
        result.textContent = 'Gagal membaca file';
      };
      reader.readAsDataURL(file);
    }

    function saveSoundSettings() {
      const soundUp = document.getElementById('soundUpUrl').value.trim();
      const soundDown = document.getElementById('soundDownUrl').value.trim();
      const result = document.getElementById('soundResult');

      result.className = 'result-msg success';
      result.textContent = 'Menyimpan...';

      fetch('/api/admin/sound-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, soundUp, soundDown })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          currentSoundUp = soundUp;
          currentSoundDown = soundDown;
          result.className = 'result-msg success';
          result.textContent = 'Sound berhasil disimpan!';
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Gagal: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      })
      .catch(e => {
        result.className = 'result-msg error';
        result.textContent = 'Error: ' + e.message;
      });
    }

    async function resetSounds() {
      const confirmed = await showConfirm('Reset semua sound ke default?', { title: 'Reset Sound', type: 'warning' });
      if (!confirmed) return;

      const result = document.getElementById('soundResult');
      result.className = 'result-msg success';
      result.textContent = 'Mereset sound...';

      fetch('/api/admin/sound-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, soundUp: '', soundDown: '' })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          currentSoundUp = '';
          currentSoundDown = '';
          document.getElementById('soundUpUrl').value = '';
          document.getElementById('soundDownUrl').value = '';
          document.getElementById('soundUpFile').value = '';
          document.getElementById('soundDownFile').value = '';
          document.getElementById('soundUpPreview').style.display = 'none';
          document.getElementById('soundDownPreview').style.display = 'none';
          result.className = 'result-msg success';
          result.textContent = 'Sound berhasil direset ke default!';
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Gagal: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function testSound(direction) {
      const url = direction === 'up'
        ? document.getElementById('soundUpUrl').value.trim()
        : document.getElementById('soundDownUrl').value.trim();

      if (url) {
        const audio = new Audio(url);
        audio.volume = 0.5;
        audio.play().catch(e => showAlert('Gagal memutar sound: ' + e.message, 'danger'));
      } else {
        playDefaultSound(direction);
      }
    }

    function playDefaultSound(direction) {
      try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        if (direction === 'up') {
          oscillator.type = 'sine';
          oscillator.frequency.setValueAtTime(800, ctx.currentTime);
          oscillator.frequency.setValueAtTime(1200, ctx.currentTime + 0.15);
          gainNode.gain.setValueAtTime(0.3, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.3);
        } else {
          oscillator.type = 'sawtooth';
          oscillator.frequency.setValueAtTime(400, ctx.currentTime);
          oscillator.frequency.exponentialRampToValueAtTime(150, ctx.currentTime + 0.3);
          gainNode.gain.setValueAtTime(0.2, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.3);
        }
      } catch (e) {
        console.log('Sound error:', e);
      }
    }

    // ==================== Admin Phones Functions ====================
    function loadAdminPhones() {
      fetch('/api/admin-phones')
        .then(r => r.json())
        .then(data => {
          if (data.success && data.phones) {
            document.getElementById('adminPhone1').value = data.phones[0] ? data.phones[0].replace('62', '0') : '';
            document.getElementById('adminPhone2').value = data.phones[1] ? data.phones[1].replace('62', '0') : '';
          }
        });
    }

    function saveAdminPhones() {
      const phone1 = document.getElementById('adminPhone1').value.trim();
      const phone2 = document.getElementById('adminPhone2').value.trim();
      const result = document.getElementById('adminPhoneResult');

      if (!phone1) {
        result.className = 'result-msg error';
        result.textContent = 'Nomor admin 1 wajib diisi';
        return;
      }

      const phones = [phone1];
      if (phone2) phones.push(phone2);

      fetch('/api/admin-phones', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phones })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Nomor admin berhasil disimpan';
          loadAdminPhones();
        } else {
          result.className = 'result-msg error';
          result.textContent = data.message || 'Gagal menyimpan';
        }
      });
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
        showAlert('Pilih grup terlebih dahulu', 'warning');
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

    async function clearInvalidUsers() {
      const confirmed = await showConfirm('Hapus semua user dengan nomor invalid (bukan format Indonesia 08xx)?', { title: 'Hapus User Invalid', type: 'warning' });
      if (!confirmed) return;

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

    async function clearAllUsers() {
      const confirmed1 = await showConfirm('HAPUS SEMUA USER? Aksi ini tidak dapat dibatalkan!', { title: 'Peringatan', type: 'danger', confirmText: 'Lanjutkan' });
      if (!confirmed1) return;
      const confirmed2 = await showConfirm('Konfirmasi sekali lagi untuk HAPUS SEMUA USER', { title: 'Konfirmasi Final', type: 'danger', confirmText: 'Hapus Semua' });
      if (!confirmed2) return;

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

    async function forceLogoutAll() {
      const confirmed = await showConfirm('Force logout semua user? Semua user akan diminta login ulang.', { title: 'Force Logout', type: 'warning' });
      if (!confirmed) return;

      const result = document.getElementById('syncResult');
      result.className = 'result-msg success';
      result.textContent = 'Memproses force logout...';

      fetch('/api/admin/force-logout-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Semua user berhasil di-logout. Mereka harus login ulang.';
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    // Load pending registrations
    function loadPendingRegistrations() {
      fetch('/api/pending-registrations')
        .then(r => r.json())
        .then(data => {
          const list = data.registrations || [];
          const tbody = document.getElementById('pendingList');
          const countEl = document.getElementById('pendingCount');
          const badgeEl = document.getElementById('pendingBadge');

          countEl.textContent = list.length;
          badgeEl.textContent = list.length;
          badgeEl.style.display = list.length > 0 ? 'inline' : 'none';

          if (list.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-state">Tidak ada pendaftaran baru</td></tr>';
            return;
          }

          tbody.innerHTML = list.map(r => {
            const time = new Date(r.timestamp).toLocaleString('id-ID');
            return '<tr style="background:rgba(247,147,26,0.08);">' +
              '<td>' + time + '</td>' +
              '<td><strong>' + r.name + '</strong></td>' +
              '<td class="phone">+' + r.phone + '</td>' +
              '<td>' +
                '<div class="action-btns">' +
                  '<button class="action-btn unblock btn-approve" data-phone="' + r.phone + '">ACC</button>' +
                  '<button class="action-btn delete btn-reject" data-phone="' + r.phone + '">Tolak</button>' +
                '</div>' +
              '</td>' +
            '</tr>';
          }).join('');

          // Add click handlers
          tbody.querySelectorAll('.btn-approve').forEach(function(btn) {
            btn.addEventListener('click', function() { approveRegistration(this.dataset.phone); });
          });
          tbody.querySelectorAll('.btn-reject').forEach(function(btn) {
            btn.addEventListener('click', function() { rejectRegistration(this.dataset.phone); });
          });
        });
    }

    async function approveRegistration(phone) {
      const confirmed = await showConfirm('Setujui pendaftaran ini?', { title: 'Setujui Pendaftaran', type: 'info', confirmText: 'Setujui' });
      if (!confirmed) return;

      fetch('/api/approve-registration', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone })
      })
      .then(r => r.json())
      .then(data => {
        showAlert(data.message, 'success');
        loadPendingRegistrations();
        loadUsers();
      });
    }

    async function rejectRegistration(phone) {
      const confirmed = await showConfirm('Tolak pendaftaran ini?', { title: 'Tolak Pendaftaran', type: 'warning', confirmText: 'Tolak' });
      if (!confirmed) return;

      fetch('/api/reject-registration', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone, reason: '' })
      })
      .then(r => r.json())
      .then(data => {
        showAlert(data.message, 'success');
        loadPendingRegistrations();
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
          let pinChanged = users.filter(u => u.pinChanged).length;

          document.getElementById('totalUsers').textContent = total;
          document.getElementById('activeUsers').textContent = active;
          document.getElementById('pushUsers').textContent = push;
          document.getElementById('pinChangedUsers').textContent = pinChanged;

          const tbody = document.getElementById('userList');
          if (users.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-state">Belum ada user</td></tr>';
            return;
          }

          tbody.innerHTML = users.map(u => {
            let status, statusClass;
            if (u.isBlocked) {
              status = 'Blocked';
              statusClass = 'status-blocked';
            } else if (!u.expired) {
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
            const pinStatus = u.pinChanged
              ? '<span class="pin-badge pin-changed">Changed</span>'
              : '<span class="pin-badge pin-default">Default</span>';
            const blockBtn = u.isBlocked
              ? '<button class="action-btn unblock" onclick="unblockUser(\\'' + u.phone + '\\')">Unblock</button>'
              : '<button class="action-btn block" onclick="blockUser(\\'' + u.phone + '\\')">Block</button>';

            return '<tr' + (u.isBlocked ? ' style="opacity:0.6;background:rgba(255,82,82,0.05);"' : '') + '>' +
              '<td class="phone">+' + u.phone + '</td>' +
              '<td>' + (u.name || '-') + '</td>' +
              '<td><span class="status-badge ' + statusClass + '">' + status + '</span></td>' +
              '<td><span class="push-badge ' + (u.hasPushSubscription ? 'push-yes' : 'push-no') + '"></span></td>' +
              '<td>' + pinStatus + '</td>' +
              '<td>' + expDate + '</td>' +
              '<td>' +
                '<div class="action-btns">' +
                  '<button class="action-btn edit" onclick="editUser(\\'' + u.phone + '\\',\\'' + (u.name||'') + '\\')">Edit</button>' +
                  '<button class="action-btn push" onclick="openPushModal(\\'' + u.phone + '\\')">Push</button>' +
                  '<button class="action-btn pin" onclick="resetPin(\\'' + u.phone + '\\')">Reset PIN</button>' +
                  blockBtn +
                  '<button class="action-btn delete" onclick="deleteUser(\\'' + u.phone + '\\')">Hapus</button>' +
                  '<button class="action-btn kick" onclick="kickUser(\\'' + u.phone + '\\')">Kick</button>' +
                '</div>' +
              '</td>' +
            '</tr>';
          }).join('');
        });
    }

    // Reset PIN user ke default (000000)
    async function resetPin(phone) {
      const confirmed = await showConfirm('Reset PIN user +62' + phone + ' ke default (000000)?', { title: 'Reset PIN', type: 'warning' });
      if (!confirmed) return;

      fetch('/api/admin/reset-pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          showAlert('PIN berhasil direset ke 000000', 'success');
          loadUsers();
        } else {
          showAlert('Error: ' + data.error, 'danger');
        }
      });
    }

    function addUser() {
      const phone = document.getElementById('newPhone').value.trim();
      const name = document.getElementById('newName').value.trim();
      const expiredDate = document.getElementById('newExpiredDate').value;
      const result = document.getElementById('addResult');

      if (!phone) { showAlert('Nomor WA wajib diisi', 'warning'); return; }

      const bodyData = {
        password: adminPass,
        phone,
        name
      };

      // If date is set, convert to timestamp
      if (expiredDate) {
        bodyData.expiredTimestamp = new Date(expiredDate + 'T23:59:59').getTime();
      }

      fetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyData)
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'User berhasil ditambahkan!';
          document.getElementById('newPhone').value = '';
          document.getElementById('newName').value = '';
          document.getElementById('newExpiredDate').value = '';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = data.error;
        }
        setTimeout(() => result.className = 'result-msg', 3000);
      });
    }

    function bulkImport() {
      const text = document.getElementById('bulkPhones').value.trim();
      const result = document.getElementById('bulkResult');

      if (!text) { showAlert('Masukkan daftar nomor', 'warning'); return; }

      // Parse phones - support newline, comma, or space separated
      const phones = text.split(/[\\n,\\s]+/).map(p => p.trim()).filter(p => p.length > 0);

      if (phones.length === 0) { showAlert('Tidak ada nomor valid', 'warning'); return; }

      result.className = 'result-msg success';
      result.textContent = 'Mengimport ' + phones.length + ' nomor...';

      fetch('/api/admin/users/bulk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phones })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = 'Import selesai! ' + data.added + ' ditambahkan, ' + data.skipped + ' dilewati.';
          document.getElementById('bulkPhones').value = '';
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
      });
    }

    function editUser(phone, name, expired) {
      document.getElementById('editPhone').value = phone;
      document.getElementById('editName').value = name;
      document.getElementById('editAddDays').value = '';
      // Set expired date if exists
      if (expired && expired !== 'Lifetime') {
        // Parse from timestamp or date string
        const expDate = new Date(expired);
        if (!isNaN(expDate.getTime())) {
          document.getElementById('editExpiredDate').value = expDate.toISOString().split('T')[0];
        } else {
          document.getElementById('editExpiredDate').value = '';
        }
      } else {
        document.getElementById('editExpiredDate').value = '';
      }
      document.getElementById('editModal').classList.add('show');
    }

    function closeModal() {
      document.getElementById('editModal').classList.remove('show');
    }

    function saveUser() {
      const phone = document.getElementById('editPhone').value;
      const name = document.getElementById('editName').value;
      const addDays = document.getElementById('editAddDays').value;
      const expiredDate = document.getElementById('editExpiredDate').value;

      const bodyData = {
        password: adminPass,
        phone,
        name
      };

      // If date is set, use it
      if (expiredDate) {
        bodyData.expiredTimestamp = new Date(expiredDate + 'T23:59:59').getTime();
      } else if (addDays) {
        bodyData.addDays = parseInt(addDays);
      }

      fetch('/api/admin/users', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyData)
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          closeModal();
          loadUsers();
          showAlert('User berhasil diupdate!', 'success');
        } else {
          showAlert(data.error || 'Gagal update user', 'danger');
        }
      });
    }

    async function deleteUser(phone) {
      const confirmed = await showConfirm('Hapus user +62' + phone + '?', { title: 'Hapus User', type: 'danger', confirmText: 'Hapus' });
      if (!confirmed) return;

      fetch('/api/admin/users', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) loadUsers();
        else showAlert(data.error, 'danger');
      });
    }

    async function blockUser(phone) {
      const confirmed = await showConfirm('Blokir user +62' + phone + '? User tidak bisa login sampai di-unblock.', { title: 'Blokir User', type: 'danger', confirmText: 'Blokir' });
      if (!confirmed) return;

      fetch('/api/admin/users/block', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          showAlert('User berhasil diblokir', 'success');
          loadUsers();
        } else {
          showAlert(data.error, 'danger');
        }
      });
    }

    async function unblockUser(phone) {
      const confirmed = await showConfirm('Buka blokir user +62' + phone + '?', { title: 'Unblock User', type: 'info', confirmText: 'Unblock' });
      if (!confirmed) return;

      fetch('/api/admin/users/unblock', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          showAlert('User berhasil di-unblock', 'success');
          loadUsers();
        } else {
          showAlert(data.error, 'danger');
        }
      });
    }

    async function kickUser(phone) {
      const confirmed = await showConfirm('KICK +62' + phone + ' dari grup WhatsApp? User akan di-kick dari grup DAN dihapus dari database!', { title: 'Kick User', type: 'danger', confirmText: 'Kick' });
      if (!confirmed) return;

      const result = document.getElementById('syncResult');
      result.className = 'result-msg success';
      result.textContent = 'Mengeluarkan user dari grup...';

      fetch('/api/admin/users/kick', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          result.className = 'result-msg success';
          result.textContent = data.message;
          loadUsers();
        } else {
          result.className = 'result-msg error';
          result.textContent = 'Error: ' + data.error;
        }
        setTimeout(() => result.className = 'result-msg', 5000);
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

      if (!title || !message) { showAlert('Judul dan pesan wajib diisi', 'warning'); return; }

      fetch('/api/admin/push', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: adminPass, phone: phone || null, type, title, message })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          showAlert('Notifikasi terkirim ke ' + data.sent + ' user', 'success');
          closePushModal();
        } else {
          showAlert(data.error, 'danger');
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
  <meta name="theme-color" content="#0a0e13">
  <meta name="mobile-web-app-capable" content="yes">
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
  <link rel="manifest" href="/manifest.json">
  <link rel="apple-touch-icon" href="/icon.png">
  <link rel="icon" type="image/x-icon" href="/favicon.ico">
  <link rel="icon" type="image/png" href="/icon.png">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
  <title>Gold Price Monitor</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: linear-gradient(180deg, #0a0e13 0%, #0f1419 100%);
      min-height: 100vh;
      padding: 16px;
      color: #e7e9ea;
    }
    .container { max-width: 1400px; width: 100%; margin: 0 auto; }

    /* Header - Modern */
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 20px;
      padding: 16px 20px;
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.06);
      box-shadow: 0 4px 24px rgba(0,0,0,0.2);
    }
    .header-left h1 {
      font-size: 1.4em;
      font-weight: 700;
      color: #ffffff;
      margin-bottom: 4px;
      display: flex;
      align-items: center;
      gap: 12px;
      letter-spacing: -0.02em;
    }
    .header-left h1 svg { color: #f7931a; flex-shrink: 0; }
    .header-left .subtitle {
      font-size: 0.85em;
      color: #8b949e;
      font-weight: 400;
    }

    /* Sound Toggle in Header */
    .sound-toggle-header {
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 8px 12px;
      background: rgba(74,222,128,0.12);
      border: 1px solid rgba(74,222,128,0.3);
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.2s ease;
      margin-left: 8px;
    }
    .sound-toggle-header:hover {
      background: rgba(74,222,128,0.2);
      transform: scale(1.05);
    }
    .sound-toggle-header svg { color: #4ade80; }
    .sound-toggle-header.off {
      background: rgba(239,68,68,0.12);
      border-color: rgba(239,68,68,0.3);
    }
    .sound-toggle-header.off svg { color: #f87171; }

    .header-right {
      text-align: right;
      display: flex;
      align-items: center;
      gap: 12px;
    }

    /* Install Button */
    .install-btn {
      display: none;
      align-items: center;
      gap: 8px;
      padding: 10px 16px;
      background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%);
      color: #000;
      border: none;
      border-radius: 10px;
      font-size: 0.85em;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s ease;
      font-family: inherit;
      box-shadow: 0 4px 15px rgba(247,147,26,0.3);
    }
    .install-btn:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(247,147,26,0.4);
    }
    .install-btn svg {
      width: 16px;
      height: 16px;
    }

    /* Logout Button */
    .logout-btn {
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 10px;
      background: rgba(239,68,68,0.15);
      color: #f87171;
      border: 1px solid rgba(239,68,68,0.3);
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .logout-btn:hover {
      background: rgba(239,68,68,0.25);
      transform: scale(1.05);
    }

    /* Stat Items */
    .stat-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 12px 18px;
      background: rgba(20, 26, 34, 0.6);
      backdrop-filter: blur(10px);
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.06);
      transition: all 0.2s ease;
    }
    .stat-item:hover {
      background: rgba(20, 26, 34, 0.8);
      border-color: rgba(255,255,255,0.1);
    }
    .stat-item .stat-label {
      font-size: 0.75em;
      color: #8b949e;
      text-transform: uppercase;
      font-weight: 600;
      letter-spacing: 0.5px;
    }
    .stat-item .stat-value {
      font-size: 1.1em;
      font-weight: 700;
      color: #ffffff;
      font-family: 'JetBrains Mono', monospace;
    }
    .stat-item .stat-value.green { color: #4ade80; }
    .stat-item .stat-value.blue { color: #60a5fa; }
    .stat-item .stat-change {
      font-size: 0.8em;
      padding: 4px 10px;
      border-radius: 6px;
      font-weight: 600;
    }
    .stat-item .stat-change.up {
      color: #4ade80;
      background: rgba(74, 222, 128, 0.12);
    }
    .stat-item .stat-change.down {
      color: #f87171;
      background: rgba(248, 113, 113, 0.12);
    }
    .stat-item.price-up { border-color: rgba(74, 222, 128, 0.4); box-shadow: 0 0 20px rgba(74, 222, 128, 0.1); }
    .stat-item.price-up .stat-value { color: #4ade80; }
    .stat-item.price-down { border-color: rgba(248, 113, 113, 0.4); box-shadow: 0 0 20px rgba(248, 113, 113, 0.1); }
    .stat-item.price-down .stat-value { color: #f87171; }
    .stat-item.invest .stat-label { color: #f7931a; }

    /* Chart Section */
    .chart-section {
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-radius: 20px;
      border: 1px solid rgba(255,255,255,0.06);
      overflow: hidden;
      margin-bottom: 24px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .chart-header {
      padding: 16px 20px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 14px;
    }
    .chart-title {
      display: flex;
      align-items: center;
      gap: 12px;
      width: 100%;
      justify-content: center;
    }
    .chart-header h2 {
      font-size: 1.2em;
      font-weight: 700;
      color: #ffffff;
      margin: 0;
      letter-spacing: -0.02em;
    }
    .chart-header .live-badge {
      background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
      color: #fff;
      font-size: 0.7em;
      padding: 5px 12px;
      border-radius: 20px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      box-shadow: 0 2px 10px rgba(34,197,94,0.3);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.7; }
    }
    .chart-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: center;
    }
    .daily-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      justify-content: center;
      padding: 12px 16px;
      background: rgba(0,0,0,0.15);
      border-top: 1px solid rgba(255,255,255,0.06);
    }
    .daily-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 14px;
      background: rgba(15, 20, 25, 0.8);
      border-radius: 10px;
      font-size: 0.85em;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .daily-item .daily-label {
      color: #8b949e;
      text-transform: uppercase;
      font-size: 0.75em;
      font-weight: 600;
      letter-spacing: 0.5px;
    }
    .daily-item .daily-value {
      color: #ffffff;
      font-weight: 600;
      font-family: 'JetBrains Mono', monospace;
    }
    .daily-item.clock-item {
      flex-direction: column;
      gap: 4px;
      padding: 10px 16px;
      background: linear-gradient(135deg, rgba(247,147,26,0.1), rgba(247,147,26,0.05));
      border: 1px solid rgba(247,147,26,0.2);
      border-radius: 12px;
    }
    /* Info Row - Clock & User Phone */
    .chart-info-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 12px;
      padding: 12px 16px;
      background: linear-gradient(135deg, rgba(247,147,26,0.08), rgba(247,147,26,0.03));
      border: 1px solid rgba(247,147,26,0.15);
      border-radius: 10px;
    }
    .info-item {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .clock-info {
      flex-direction: column;
      align-items: flex-start;
      gap: 2px;
    }
    .info-time {
      font-size: 1.15em;
      font-weight: 700;
      color: #f7931a;
      font-family: 'JetBrains Mono', monospace;
      letter-spacing: 1px;
    }
    .info-date {
      font-size: 0.75em;
      color: #8b949e;
    }
    .user-info-display {
      background: rgba(255,255,255,0.05);
      padding: 8px 14px;
      border-radius: 8px;
    }
    .info-label {
      font-size: 0.8em;
      color: #8b949e;
    }
    .info-value {
      font-size: 0.95em;
      font-weight: 600;
      color: #4ade80;
      font-family: 'JetBrains Mono', monospace;
    }
    .clock-time {
      font-size: 1.3em;
      font-weight: 600;
      color: #f7931a;
      font-family: 'JetBrains Mono', monospace;
      letter-spacing: 1px;
    }
    .clock-date {
      font-size: 0.8em;
      color: #8b949e;
    }
    .trend-icon-up {
      color: #4ade80;
      font-size: 1.2em;
    }
    .trend-icon-down {
      color: #f87171;
      font-size: 1.2em;
    }
    .daily-item .daily-value.high { color: #4ade80; }
    .daily-item .daily-value.low { color: #f87171; }
    .daily-item.sound-toggle {
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .daily-item.sound-toggle:hover {
      background: rgba(247,147,26,0.15);
      border-color: rgba(247,147,26,0.3);
    }

    /* Notification Banner */
    #notifContainer {
      display: flex;
      flex-direction: column;
      gap: 12px;
      margin-bottom: 20px;
    }
    .notif-banner {
      background: rgba(20, 26, 34, 0.9);
      backdrop-filter: blur(10px);
      border-radius: 14px;
      padding: 14px 18px;
      display: flex;
      align-items: center;
      gap: 14px;
      border-left: 4px solid #60a5fa;
      animation: slideDown 0.3s ease;
      border: 1px solid rgba(255,255,255,0.06);
    }
    .notif-banner.promo { border-left-color: #f7931a; background: linear-gradient(135deg, rgba(247,147,26,0.08) 0%, rgba(20,26,34,0.9) 100%); }
    .notif-banner.warning { border-left-color: #fbbf24; background: linear-gradient(135deg, rgba(251,191,36,0.08) 0%, rgba(20,26,34,0.9) 100%); }
    .notif-banner.urgent { border-left-color: #f87171; background: linear-gradient(135deg, rgba(248,113,113,0.08) 0%, rgba(20,26,34,0.9) 100%); }
    .notif-banner.info { border-left-color: #60a5fa; background: linear-gradient(135deg, rgba(96,165,250,0.08) 0%, rgba(20,26,34,0.9) 100%); }
    .notif-icon {
      width: 40px;
      height: 40px;
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      font-size: 18px;
    }
    .notif-banner.promo .notif-icon { background: linear-gradient(135deg, #f7931a, #e8850f); }
    .notif-banner.warning .notif-icon { background: linear-gradient(135deg, #fbbf24, #f59e0b); }
    .notif-banner.urgent .notif-icon { background: linear-gradient(135deg, #f87171, #ef4444); }
    .notif-banner.info .notif-icon { background: linear-gradient(135deg, #60a5fa, #3b82f6); }
    .notif-content {
      flex: 1;
      min-width: 0;
    }
    .notif-title {
      font-size: 0.95em;
      font-weight: 600;
      color: #ffffff;
      margin-bottom: 4px;
    }
    .notif-message {
      font-size: 0.85em;
      color: #8b949e;
      line-height: 1.4;
    }
    .notif-close {
      background: rgba(255,255,255,0.08);
      border: none;
      color: #8b949e;
      font-size: 16px;
      cursor: pointer;
      padding: 8px 12px;
      border-radius: 8px;
      transition: all 0.2s;
    }
    .notif-close:hover { background: rgba(255,255,255,0.15); color: #fff; }
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
      background: rgba(20, 26, 34, 0.8);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border-radius: 20px;
      border: 1px solid rgba(255,255,255,0.06);
      overflow: hidden;
      box-shadow: 0 8px 32px rgba(0,0,0,0.2);
    }
    .history-header {
      padding: 18px 24px;
      border-bottom: 1px solid rgba(255,255,255,0.06);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .history-header h2 {
      font-size: 1.1em;
      font-weight: 700;
      color: #ffffff;
      letter-spacing: -0.02em;
    }
    .history-header .count {
      font-size: 0.9em;
      color: #8b949e;
      font-weight: 500;
    }
    .history-table {
      width: 100%;
      border-collapse: collapse;
      min-width: 600px;
    }
    .history-table th {
      text-align: left;
      padding: 12px 10px;
      font-size: 0.75em;
      color: #8b949e;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      background: rgba(0,0,0,0.2);
      font-weight: 600;
      white-space: nowrap;
    }
    .history-table td {
      padding: 14px 10px;
      font-size: 0.9em;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      color: #e7e9ea;
      white-space: nowrap;
      font-family: 'JetBrains Mono', monospace;
    }
    .history-table tr:last-child td {
      border-bottom: none;
    }
    .history-table tr:hover {
      background: rgba(255,255,255,0.03);
    }
    .history-table .price-up { color: #4ade80; font-weight: 600; }
    .history-table .price-down { color: #f87171; font-weight: 600; }
    .history-table .time-col { color: #8b949e; font-family: 'JetBrains Mono', monospace; font-size: 0.9em; }
    .history-table .no-data {
      text-align: center;
      color: #8b949e;
      padding: 50px 20px;
      font-size: 0.95em;
    }
    .history-pagination {
      display: flex;
      justify-content: center;
      align-items: center;
      gap: 16px;
      padding: 18px 24px;
      border-top: 1px solid rgba(255,255,255,0.06);
    }
    .page-btn {
      background: rgba(255,255,255,0.08);
      color: #e7e9ea;
      border: 1px solid rgba(255,255,255,0.1);
      padding: 10px 20px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 0.9em;
      font-weight: 500;
      transition: all 0.2s ease;
      font-family: inherit;
    }
    .page-btn:hover:not(:disabled) {
      background: rgba(247,147,26,0.15);
      border-color: rgba(247,147,26,0.3);
      color: #f7931a;
    }
    .page-btn:disabled { opacity: 0.3; cursor: not-allowed; }
    .page-info { color: #8b949e; font-size: 0.9em; font-weight: 500; }

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
        background: linear-gradient(145deg, rgba(74, 222, 128, 0.25), rgba(74, 222, 128, 0.1));
        box-shadow: 0 0 30px rgba(74, 222, 128, 0.25);
      }
      100% {
        background: rgba(20, 26, 34, 0.6);
        box-shadow: none;
      }
    }
    @keyframes highlight-down {
      0%, 30% {
        background: linear-gradient(145deg, rgba(248, 113, 113, 0.25), rgba(248, 113, 113, 0.1));
        box-shadow: 0 0 30px rgba(248, 113, 113, 0.25);
      }
      100% {
        background: rgba(20, 26, 34, 0.6);
        box-shadow: none;
      }
    }
    @keyframes highlight {
      0% { background: rgba(247, 147, 26, 0.25); }
      100% { background: transparent; }
    }

    /* Responsive - Tablet */
    @media (max-width: 768px) {
      body { padding: 12px; }
      .container { max-width: 100%; }
      .header {
        flex-direction: column;
        text-align: center;
        gap: 12px;
        padding: 16px 18px;
        margin-bottom: 16px;
        border-radius: 14px;
      }
      .header-left h1 { font-size: 1.15em; flex-wrap: wrap; justify-content: center; }
      .header-right { text-align: center; flex-direction: row; flex-wrap: wrap; justify-content: center; gap: 10px; }
      .sound-toggle-header { margin-left: 0; }
      .chart-section { margin-bottom: 16px; border-radius: 16px; }
      .chart-header { padding: 14px 16px; gap: 12px; }
      .chart-header h2 { font-size: 1em; }
      .chart-stats { gap: 8px; }
      .stat-item { padding: 10px 14px; gap: 8px; border-radius: 10px; }
      .stat-item .stat-label { font-size: 0.7em; }
      .stat-item .stat-value { font-size: 0.95em; }
      .stat-item .stat-change { font-size: 0.75em; padding: 3px 8px; }
      .tradingview-widget-container { height: 400px; }
      .history-section { border-radius: 16px; }
      .history-header { padding: 14px 18px; }
      .history-header h2 { font-size: 1em; }
      .history-table th { padding: 10px; font-size: 0.7em; }
      .history-table td { padding: 12px 10px; font-size: 0.85em; }
      .history-pagination { padding: 14px; gap: 12px; }
      .page-btn { padding: 8px 16px; font-size: 0.85em; }
    }

    /* Responsive - Mobile */
    @media (max-width: 480px) {
      body { padding: 10px; }
      .header {
        padding: 14px;
        margin-bottom: 12px;
        border-radius: 12px;
      }
      .header-left h1 { font-size: 1em; }
      .header-left h1 svg { width: 18px; height: 18px; }
      .header-left .subtitle { font-size: 0.8em; }
      .sound-toggle-header { padding: 6px 10px; }
      .sound-toggle-header svg { width: 16px; height: 16px; }
      .stat-item.clock-item { min-width: 110px; }
      .stat-item.clock-item .clock-time { font-size: 0.95em; }
      .stat-item.clock-item .clock-date { font-size: 0.6em; }

      .chart-section {
        margin-bottom: 12px;
        border-radius: 14px;
      }
      .chart-header { padding: 12px 14px; gap: 10px; }
      .chart-title { gap: 8px; }
      .chart-header h2 { font-size: 0.95em; }
      .chart-header h2 svg { width: 14px; height: 14px; }
      .live-badge { font-size: 0.6em; padding: 4px 10px; }
      .chart-stats { gap: 6px; }
      .stat-item { padding: 8px 12px; gap: 6px; border-radius: 8px; }
      .stat-item .stat-label { font-size: 0.65em; }
      .stat-item .stat-value { font-size: 0.85em; }
      .stat-item .stat-change { font-size: 0.7em; padding: 2px 6px; border-radius: 4px; }
      .tradingview-widget-container { height: 350px; }

      .history-section { border-radius: 14px; }
      .history-header { padding: 12px 16px; }
      .history-header h2 { font-size: 0.95em; }
      .history-header h2 svg { width: 14px; height: 14px; }
      .history-table th { padding: 10px 8px; font-size: 0.65em; }
      .history-table td { padding: 10px 8px; font-size: 0.8em; }
      .history-pagination { padding: 12px; gap: 10px; flex-wrap: wrap; }
      .page-btn { padding: 8px 14px; font-size: 0.8em; }
      .page-info { font-size: 0.8em; }
    }

    /* Extra small screens */
    @media (max-width: 360px) {
      body { padding: 8px; }
      .header { padding: 12px; margin-bottom: 10px; }
      .header-left h1 { font-size: 1em; }
      .clock { font-size: 1.1em; }
      .chart-header { padding: 10px 12px; gap: 8px; }
      .chart-header h2 { font-size: 0.9em; }
      .stat-item { padding: 6px 10px; gap: 4px; }
      .stat-item .stat-label { font-size: 0.6em; }
      .stat-item .stat-value { font-size: 0.85em; }
      .stat-item .stat-change { font-size: 0.5em; }
      .tradingview-widget-container { height: 280px; }
      .history-table th, .history-table td { padding: 6px 8px; font-size: 1.3em; }
    }

    /* Professional Toast System */
    .toast-container {
      position: fixed;
      top: 20px;
      right: 20px;
      z-index: 9999;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .toast {
      background: rgba(20, 26, 34, 0.98);
      backdrop-filter: blur(10px);
      border-radius: 12px;
      padding: 14px 18px;
      display: flex;
      align-items: center;
      gap: 12px;
      border: 1px solid rgba(255,255,255,0.1);
      box-shadow: 0 10px 30px rgba(0,0,0,0.4);
      animation: slideIn 0.3s ease;
      max-width: 320px;
    }
    @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
    @keyframes slideOut { from { transform: translateX(0); opacity: 1; } to { transform: translateX(100%); opacity: 0; } }
    .toast-icon {
      width: 36px;
      height: 36px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
    }
    .toast-icon svg { width: 18px; height: 18px; }
    .toast.info .toast-icon { background: rgba(59,130,246,0.15); color: #60a5fa; }
    .toast.success .toast-icon { background: rgba(34,197,94,0.15); color: #4ade80; }
    .toast.warning .toast-icon { background: rgba(251,191,36,0.15); color: #fbbf24; }
    .toast.danger .toast-icon { background: rgba(239,68,68,0.15); color: #f87171; }
    .toast-message { color: #e7e9ea; font-size: 0.9em; line-height: 1.4; }

    /* Confirm Modal */
    .confirm-modal {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.85);
      backdrop-filter: blur(8px);
      align-items: center;
      justify-content: center;
      z-index: 9999;
    }
    .confirm-modal.show { display: flex; }
    .confirm-box {
      background: linear-gradient(180deg, rgba(25, 32, 42, 0.98) 0%, rgba(18, 24, 32, 0.98) 100%);
      border-radius: 16px;
      padding: 24px;
      width: 90%;
      max-width: 340px;
      text-align: center;
      border: 1px solid rgba(255,255,255,0.1);
      box-shadow: 0 25px 60px rgba(0,0,0,0.5);
    }
    .confirm-icon {
      width: 56px;
      height: 56px;
      border-radius: 50%;
      background: rgba(251,191,36,0.15);
      color: #fbbf24;
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 0 auto 16px;
    }
    .confirm-icon svg { width: 28px; height: 28px; }
    .confirm-title { color: #fff; font-size: 1.1em; font-weight: 600; margin-bottom: 8px; }
    .confirm-message { color: #9ca3af; font-size: 0.9em; line-height: 1.5; margin-bottom: 20px; }
    .confirm-buttons { display: flex; gap: 10px; justify-content: center; }
    .confirm-btn {
      padding: 10px 24px;
      border-radius: 10px;
      font-size: 0.88em;
      font-weight: 600;
      cursor: pointer;
      border: none;
      transition: all 0.2s;
    }
    .confirm-btn.cancel { background: rgba(255,255,255,0.08); color: #e7e9ea; }
    .confirm-btn.cancel:hover { background: rgba(255,255,255,0.15); }
    .confirm-btn.ok { background: linear-gradient(135deg, #f7931a 0%, #e8850f 100%); color: white; }
    .confirm-btn.ok:hover { transform: translateY(-1px); }
  </style>
</head>
<body>
  <div class="toast-container" id="toastContainer"></div>
  <div class="confirm-modal" id="confirmModal">
    <div class="confirm-box">
      <div class="confirm-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
      </div>
      <div class="confirm-title" id="confirmTitle">Konfirmasi</div>
      <div class="confirm-message" id="confirmMessage">Apakah Anda yakin?</div>
      <div class="confirm-buttons">
        <button class="confirm-btn cancel" onclick="resolveConfirm(false)">Batal</button>
        <button class="confirm-btn ok" onclick="resolveConfirm(true)">Ya</button>
      </div>
    </div>
  </div>
  <div class="container">
    <div class="header">
      <div class="header-left">
        <h1>
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#f7931a" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 6v12M8 10h8M8 14h8"/></svg>
          Gold Price Monitor
          <div class="sound-toggle-header" id="soundToggle" onclick="toggleSound()" title="Toggle Sound">
            <svg id="soundIconOn" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M15.54 8.46a5 5 0 0 1 0 7.07"/></svg>
            <svg id="soundIconOff" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="display:none;"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><line x1="23" y1="9" x2="17" y2="15"/><line x1="17" y1="9" x2="23" y2="15"/></svg>
          </div>
        </h1>
        <div class="subtitle">Real-time Treasury Gold Rates</div>
      </div>
      <div class="header-right">
        <button class="install-btn" id="installBtn" onclick="installApp()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          Install
        </button>
        <button class="logout-btn" id="logoutBtn" onclick="logout()" title="Logout">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        </button>
      </div>
    </div>

    <!-- Notification Banner Container -->
    <div id="notifContainer"></div>

    <div class="chart-section">
      <div class="chart-header">
        <div class="chart-title">
          <h2><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle;margin-right:8px;"><path d="M3 3v18h18"/><path d="M18 9l-5 5-4-4-3 3"/></svg>XAU/USD Chart <span id="trendIcon" style="margin-left:8px;"></span></h2>
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
            <span class="stat-label">10jt</span>
            <span class="stat-value" id="gram10">-</span>
            <span class="stat-change up" id="profit10">-</span>
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
        <div class="chart-info-row">
          <div class="info-item clock-info">
            <span class="info-time" id="clock2">--:--:--</span>
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
            "BB@tv-basicstudies",
            "RSI@tv-basicstudies",
            "MACD@tv-basicstudies",
            "PivotPointsStandard@tv-basicstudies"
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
      <div style="overflow-x:auto;">
      <table class="history-table">
        <thead>
          <tr>
            <th>Waktu</th>
            <th>Beli</th>
            <th>Jual</th>
            <th>Spread</th>
            <th>USD/IDR</th>
            <th>10jt</th>
            <th>20jt</th>
            <th>30jt</th>
            <th>+/-</th>
          </tr>
        </thead>
        <tbody id="historyBody">
          <tr><td colspan="9" class="no-data">Menunggu data...</td></tr>
        </tbody>
      </table>
      </div>
      <div class="history-pagination" id="historyPagination" style="display:none;">
        <button class="page-btn" id="prevPage" disabled>Sebelumnya</button>
        <span class="page-info" id="pageInfo">Halaman 1</span>
        <button class="page-btn" id="nextPage">Selanjutnya</button>
      </div>
    </div>
  </div>

  <script>
    // ==================== Toast System ====================
    const toastIcons = {
      info: '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>',
      success: '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
      warning: '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
      danger: '<circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/>'
    };

    function showToast(message, type = 'info', duration = 4000) {
      const container = document.getElementById('toastContainer');
      const toast = document.createElement('div');
      toast.className = 'toast ' + type;
      toast.innerHTML = '<div class="toast-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">' + (toastIcons[type] || toastIcons.info) + '</svg></div><div class="toast-message">' + message + '</div>';
      container.appendChild(toast);
      setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease forwards';
        setTimeout(() => toast.remove(), 300);
      }, duration);
    }

    let confirmResolver = null;
    function showConfirm(message, title = 'Konfirmasi') {
      return new Promise((resolve) => {
        confirmResolver = resolve;
        document.getElementById('confirmTitle').textContent = title;
        document.getElementById('confirmMessage').textContent = message;
        document.getElementById('confirmModal').classList.add('show');
      });
    }
    function resolveConfirm(result) {
      document.getElementById('confirmModal').classList.remove('show');
      if (confirmResolver) { confirmResolver(result); confirmResolver = null; }
    }

    const days = ['Minggu', 'Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu'];
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];

    let lastBuy = 0;
    let lastSell = 0;
    let lastUpdatedAt = 0; // Track timestamp untuk anti flip-flop
    const PER_PAGE = 10;
    let currentPage = 1;
    let totalPages = 1;
    let totalRecords = 0;
    let currentUsdIdr = 0;

    // Local history storage
    const LOCAL_HISTORY_KEY = 'gold_price_history';
    const MAX_LOCAL_HISTORY = 1440;

    function getLocalHistory() {
      try {
        const data = localStorage.getItem(LOCAL_HISTORY_KEY);
        return data ? JSON.parse(data) : [];
      } catch (e) {
        return [];
      }
    }

    function saveLocalHistory(history) {
      try {
        const trimmed = history.slice(-MAX_LOCAL_HISTORY);
        localStorage.setItem(LOCAL_HISTORY_KEY, JSON.stringify(trimmed));
      } catch (e) {}
    }

    function addToLocalHistory(entry) {
      const history = getLocalHistory();
      if (history.some(h => h.time === entry.time)) return;
      history.push(entry);
      saveLocalHistory(history);
    }

    // Load history dari localStorage
    function loadHistory() {
      const history = getLocalHistory();
      totalRecords = history.length;
      totalPages = Math.ceil(totalRecords / PER_PAGE) || 1;
      const start = Math.max(0, history.length - (currentPage * PER_PAGE));
      const end = history.length - ((currentPage - 1) * PER_PAGE);
      const items = history.slice(start, end).reverse();
      renderServerHistory(items);
    }

    function renderServerHistory(items) {
      const tbody = document.getElementById('historyBody');
      const countEl = document.getElementById('historyCount');
      const pagination = document.getElementById('historyPagination');

      if (!items || items.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" class="no-data">Belum ada data perubahan harga</td></tr>';
        countEl.textContent = '0 records';
        pagination.style.display = 'none';
        return;
      }

      countEl.textContent = totalRecords + ' records';

      let html = '';
      items.forEach(function(item, index) {
        const time = new Date(item.time);
        const timeStr = time.toTimeString().substring(0, 8);
        const buyChange = item.buyChange || 0;
        const changeSign = buyChange >= 0 ? '+' : '';
        const changeClass = buyChange >= 0 ? 'price-up' : 'price-down';

        // Calculate spread (if not in data, calculate it)
        const spread = item.spread || ((item.sell - item.buy) / item.buy * 100).toFixed(2);
        const spreadClass = parseFloat(spread) < 0 ? 'price-down' : '';

        // USD/IDR - use current rate as fallback for old entries
        const usdIdrVal = item.usdIdr || currentUsdIdr;
        const usdIdr = usdIdrVal ? Math.round(usdIdrVal).toLocaleString('id-ID') : '-';

        // Calculate gram for 10jt, 20jt and 30jt based on buy price
        const gram10jt = 10000000 / item.buy;
        const gram20jt = 20000000 / item.buy;
        const gram30jt = 30000000 / item.buy;

        // Calculate profit: (gram * harga_jual) - (modal - potongan)
        // Potongan: 10jt=3.5%, 20jt=3.425%, 30jt=3.4%
        const profit10jt = Math.round((gram10jt * item.sell) - (10000000 - 10000000 * 0.035));
        const profit20jt = Math.round((gram20jt * item.sell) - (20000000 - 20000000 * 0.03425));
        const profit30jt = Math.round((gram30jt * item.sell) - (30000000 - 30000000 * 0.034));

        const profitClass10 = profit10jt >= 0 ? 'price-up' : 'price-down';
        const profitClass20 = profit20jt >= 0 ? 'price-up' : 'price-down';
        const profitClass30 = profit30jt >= 0 ? 'price-up' : 'price-down';
        const profitSign10 = profit10jt >= 0 ? '+' : '';
        const profitSign20 = profit20jt >= 0 ? '+' : '';
        const profitSign30 = profit30jt >= 0 ? '+' : '';

        html += '<tr>' +
          '<td class="time-col">' + timeStr + '</td>' +
          '<td>' + formatRupiahShort(item.buy) + '</td>' +
          '<td>' + formatRupiahShort(item.sell) + '</td>' +
          '<td class="' + spreadClass + '">' + spread + '%</td>' +
          '<td>' + usdIdr + '</td>' +
          '<td><span style="color:#e7e9ea;">' + gram10jt.toFixed(4) + 'g</span><br><small class="' + profitClass10 + '">' + profitSign10 + 'Rp ' + Math.abs(profit10jt).toLocaleString('id-ID') + '</small></td>' +
          '<td><span style="color:#e7e9ea;">' + gram20jt.toFixed(4) + 'g</span><br><small class="' + profitClass20 + '">' + profitSign20 + 'Rp ' + Math.abs(profit20jt).toLocaleString('id-ID') + '</small></td>' +
          '<td><span style="color:#e7e9ea;">' + gram30jt.toFixed(4) + 'g</span><br><small class="' + profitClass30 + '">' + profitSign30 + 'Rp ' + Math.abs(profit30jt).toLocaleString('id-ID') + '</small></td>' +
          '<td class="' + changeClass + '">' + changeSign + formatChangeShort(buyChange) + '</td>' +
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

    function formatRupiahShort(n) {
      // Format lengkap: 2.325.000 -> Rp 2.325.000
      return 'Rp ' + n.toLocaleString('id-ID');
    }

    function formatChangeShort(n) {
      // Format singkat untuk perubahan
      const abs = Math.abs(n);
      if (abs >= 1000) {
        return (n / 1000).toFixed(1) + 'rb';
      }
      return n.toLocaleString('id-ID');
    }

    function formatTime(date) {
      const h = date.getHours().toString().padStart(2, '0');
      const m = date.getMinutes().toString().padStart(2, '0');
      const s = date.getSeconds().toString().padStart(2, '0');
      return h + ':' + m + ':' + s;
    }

    // Daily Statistics - fetch dari server
    // Sound Notification - menggunakan audio file dari admin
    let soundEnabled = localStorage.getItem('soundEnabled') !== 'false';
    let audioContext = null;
    let customSoundUp = '';
    let customSoundDown = '';

    // Initialize sound icon state on load
    (function initSoundIcon() {
      const toggle = document.getElementById('soundToggle');
      const iconOn = document.getElementById('soundIconOn');
      const iconOff = document.getElementById('soundIconOff');
      if (toggle && iconOn && iconOff) {
        if (!soundEnabled) {
          toggle.classList.add('off');
          iconOn.style.display = 'none';
          iconOff.style.display = 'block';
        }
      }
    })();

    // Load custom sounds from server
    async function loadCustomSounds() {
      try {
        const res = await fetch('/api/sound-settings');
        const data = await res.json();
        if (data.success) {
          customSoundUp = data.settings.soundUp || '';
          customSoundDown = data.settings.soundDown || '';
        }
      } catch (e) {}
    }
    loadCustomSounds();

    function getAudioContext() {
      if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
      }
      return audioContext;
    }

    function toggleSound() {
      soundEnabled = !soundEnabled;
      localStorage.setItem('soundEnabled', soundEnabled);
      const toggle = document.getElementById('soundToggle');
      const iconOn = document.getElementById('soundIconOn');
      const iconOff = document.getElementById('soundIconOff');
      if (soundEnabled) {
        toggle.classList.remove('off');
        iconOn.style.display = 'block';
        iconOff.style.display = 'none';
      } else {
        toggle.classList.add('off');
        iconOn.style.display = 'none';
        iconOff.style.display = 'block';
      }
    }

    // Play default beep sound using Web Audio API
    function playDefaultSound(direction) {
      try {
        const ctx = getAudioContext();
        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        if (direction === 'up') {
          oscillator.type = 'sine';
          oscillator.frequency.setValueAtTime(800, ctx.currentTime);
          oscillator.frequency.setValueAtTime(1200, ctx.currentTime + 0.15);
          gainNode.gain.setValueAtTime(0.3, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.3);
        } else {
          oscillator.type = 'sawtooth';
          oscillator.frequency.setValueAtTime(400, ctx.currentTime);
          oscillator.frequency.exponentialRampToValueAtTime(150, ctx.currentTime + 0.3);
          gainNode.gain.setValueAtTime(0.2, ctx.currentTime);
          gainNode.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
          oscillator.start(ctx.currentTime);
          oscillator.stop(ctx.currentTime + 0.3);
        }
      } catch (e) {
        console.log('Sound error:', e);
      }
    }

    function playSound(direction) {
      if (!soundEnabled) return;

      const soundUrl = direction === 'up' ? customSoundUp : customSoundDown;

      if (soundUrl) {
        // Play custom sound from admin
        const audio = new Audio(soundUrl);
        audio.volume = 0.5;
        audio.play().catch(e => {
          console.log('Custom sound error, using default:', e);
          playDefaultSound(direction);
        });
      } else {
        // Play default beep sound
        playDefaultSound(direction);
      }
    }

    // Browser Notification
    let notifEnabled = false;

    async function requestNotificationPermission() {
      if (!('Notification' in window)) {
        showToast('Browser tidak mendukung notifikasi', 'warning');
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

      // Play sound for promo
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

    // Disable right-click
    // Right-click enabled

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

    // Logout function
    async function logout() {
      const confirmed = await showConfirm('Yakin ingin keluar?', 'Logout');
      if (!confirmed) return;

      const session = localStorage.getItem('goldmonitor_session');
      if (session) {
        try {
          await fetch('/api/logout', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session })
          });
        } catch (e) {}
        localStorage.removeItem('goldmonitor_session');
      }
      window.location.replace('/login');
    }

    // Check session validity and PIN status on page load
    (function checkSession() {
      const session = localStorage.getItem('goldmonitor_session');
      if (!session) {
        window.location.replace('/login');
        return;
      }

      fetch('/api/verify-session?session=' + session)
        .then(r => r.json())
        .then(data => {
          if (!data.valid) {
            localStorage.removeItem('goldmonitor_session');
            window.location.replace('/login');
            return;
          }
          // Check if PIN change is required
          return fetch('/api/check-pin-status?session=' + session);
        })
        .then(r => r ? r.json() : null)
        .then(pinData => {
          if (pinData && pinData.requirePinChange) {
            // Redirect to login to change PIN
            window.location.replace('/login');
          }
        })
        .catch(() => {});
    })();

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
      const timeStr = formatTime(now);
      const dayName = days[now.getDay()];
      const date = now.getDate();
      const month = months[now.getMonth()];
      const year = now.getFullYear();
      const dateStr = dayName + ', ' + date + ' ' + month + ' ' + year + ' WIB';
      
      // Update clock2 di pojok kanan (bawah Sound)
      const clock2 = document.getElementById('clock2');
      const dateInfo2 = document.getElementById('dateInfo2');
      if (clock2) clock2.textContent = timeStr;
      if (dateInfo2) dateInfo2.textContent = dateStr;
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

        // Anti flip-flop: cek timestamp
        const dataTimestamp = data.updatedAt ? new Date(data.updatedAt).getTime() : 0;
        if (dataTimestamp > 0 && dataTimestamp <= lastUpdatedAt) {
          return; // Skip data lama
        }
        if (dataTimestamp > lastUpdatedAt) {
          lastUpdatedAt = dataTimestamp;
        }

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
      // Include session for online user tracking
      const session = localStorage.getItem('goldmonitor_session') || '';
      evtSource = new EventSource('/sse?session=' + encodeURIComponent(session));
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

        // Skip heartbeat silently
        if (data.type === 'heartbeat') {
          return;
        }

        // Handle notifikasi/promo dari admin
        if (data.type === 'notification') {
          showPromoNotification(data);
          return;
        }

        // Handle sound settings update from admin
        if (data.type === 'sound_update') {
          customSoundUp = data.settings.soundUp || '';
          customSoundDown = data.settings.soundDown || '';
          console.log('Sound settings updated');
          return;
        }

        // Handle force logout from admin
        if (data.type === 'force_logout') {
          showToast('Sesi Anda telah berakhir. Silakan login kembali.', 'warning');
          setTimeout(() => {
            localStorage.removeItem('goldmonitor_session');
            window.location.href = '/login';
          }, 2000);
          return;
        }

        if (data.type === 'price') {
          // Anti flip-flop: cek timestamp, skip jika data lama
          const dataTimestamp = data.updatedAt ? new Date(data.updatedAt).getTime() : 0;
          if (dataTimestamp > 0 && dataTimestamp <= lastUpdatedAt) {
            return; // Skip data lama
          }
          if (dataTimestamp > lastUpdatedAt) {
            lastUpdatedAt = dataTimestamp;
          }

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

              // Update trend icon di XAU/USD Chart title
              const trendIcon = document.getElementById('trendIcon');
              if (trendIcon) {
                if (change > 0) {
                  trendIcon.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="#00c853" style="vertical-align:middle;"><path d="M7 14l5-5 5 5H7z"/></svg>';
                  trendIcon.className = 'trend-icon-up';
                } else {
                  trendIcon.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="#ff5252" style="vertical-align:middle;"><path d="M7 10l5 5 5-5H7z"/></svg>';
                  trendIcon.className = 'trend-icon-down';
                }
              }

              // Browser Notification
              const notifTitle = change > 0 ? 'Harga Emas NAIK' : 'Harga Emas TURUN';
              const notifBody = 'Rp ' + data.buy.toLocaleString('id-ID') + ' (' + sign + change.toLocaleString('id-ID') + ')';
              showNotification(notifTitle, notifBody, change > 0);

              const buyCard = document.getElementById('buyCard');
              buyCard.classList.remove('updated', 'updated-up', 'updated-down', 'price-up', 'price-down');
              void buyCard.offsetWidth;
              buyCard.classList.add(change > 0 ? 'updated-up' : 'updated-down', change > 0 ? 'price-up' : 'price-down');

              // Save ke localStorage
              addToLocalHistory({
                time: data.updatedAt,
                buy: data.buy,
                sell: data.sell,
                buyChange: change,
                sellChange: data.sell - (data.prevSell || data.sell),
                spread: ((data.sell - data.buy) / data.buy * 100).toFixed(2),
                usdIdr: data.usdIdr || 0
              });
              updateHistory();
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

            const gram10 = 10000000 / data.buy;
            const gram20 = 20000000 / data.buy;
            const gram30 = 30000000 / data.buy;
            const profit10 = (gram10 * data.sell) - (10000000 - 10000000 * 0.035);
            const profit20 = (gram20 * data.sell) - (20000000 - 20000000 * 0.03425);
            const profit30 = (gram30 * data.sell) - (30000000 - 30000000 * 0.034);

            document.getElementById('gram10').textContent = gram10.toFixed(4) + ' gr';
            document.getElementById('gram20').textContent = gram20.toFixed(4) + ' gr';
            document.getElementById('gram30').textContent = gram30.toFixed(4) + ' gr';
            document.getElementById('profit10').textContent = '+Rp ' + Math.round(profit10).toLocaleString('id-ID');
            document.getElementById('profit20').textContent = '+Rp ' + Math.round(profit20).toLocaleString('id-ID');
            document.getElementById('profit30').textContent = '+Rp ' + Math.round(profit30).toLocaleString('id-ID');
          }
        }
      } catch (e) {}
    };

    evtSource.onopen = function() {
      const badge = document.querySelector('.live-badge');
      if (badge) { badge.textContent = 'Live'; badge.style.background = 'linear-gradient(135deg, #22c55e 0%, #16a34a 100%)'; }
      lastDataTime = Date.now();
      sseReconnectCount = 0; // Reset reconnect count on successful connection
    };

    evtSource.onerror = function() {
      const badge = document.querySelector('.live-badge');
      if (badge) { badge.textContent = 'Reconnecting...'; badge.style.background = '#f59e0b'; }
      sseReconnectCount++;
      // Auto reload setelah 5 detik reconnect untuk fresh connection
      if (sseReconnectCount >= 2) {
        badge.textContent = 'Reloading...';
        badge.style.background = '#ef4444';
        setTimeout(function() {
          window.location.reload();
        }, 5000);
      }
    };
    } // end setupSSEHandlers

    // Reconnect counter
    let sseReconnectCount = 0;

    // Start SSE connection
    connectSSE();

    // Check jika tidak ada data selama 60 detik, reconnect
    setInterval(function() {
      if (Date.now() - lastDataTime > 60000) {
        sseReconnectCount++;
        if (sseReconnectCount >= 3) {
          // Auto reload jika sudah reconnect 3x tanpa data
          window.location.reload();
        } else {
          connectSSE();
        }
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

// ==================== CATCH-ALL ROUTE ====================
// Semua route yang tidak terdaftar akan redirect ke /login
app.get('*', (_req, res) => {
  res.redirect('/login')
})



// ====== AUTO-KICK EXPIRED USERS ======
async function checkAndKickExpiredUsers() {
  try {
    const allUsers = await redis.hgetall(REDIS_KEYS.USERS)
    if (!allUsers) return

    const now = Date.now()

    for (const [phone, userData] of Object.entries(allUsers)) {
      try {
        const user = typeof userData === 'string' ? JSON.parse(userData) : userData

        // Check if expired
        if (user.expired && user.expired < now) {
          pushLog(`Auto-kick | User +${phone} expired, processing...`)

          // Try to kick from group if connected
          if (sock && isReady && monitoredGroupId) {
            try {
              const jid = phone + '@s.whatsapp.net'
              await sock.groupParticipantsUpdate(monitoredGroupId, [jid], 'remove')
              pushLog(`Auto-kick | Kicked +${phone} from group`)

              // Send expiry notification
              try {
                await sock.sendMessage(jid, {
                  text: `⏰ *LANGGANAN EXPIRED*\n\nHalo ${user.name || 'User'},\n\nLangganan Anda telah berakhir pada ${new Date(user.expired).toLocaleDateString('id-ID')}.\n\nAnda telah dikeluarkan dari grup.\n\nUntuk perpanjang, hubungi admin:\nhttps://wa.me/6289654454210`
                })
              } catch (msgErr) {}
            } catch (kickErr) {
              pushLog(`Auto-kick | Failed to kick +${phone}: ${kickErr.message}`)
            }
          }

          // Delete from database
          await redis.hdel(REDIS_KEYS.USERS, phone)
          await redis.hdel(REDIS_KEYS.PUSH_SUBS, phone)

          // Remove sessions
          const sessions = await redis.hgetall(REDIS_KEYS.SESSIONS)
          for (const [sessId, sessPhone] of Object.entries(sessions || {})) {
            if (sessPhone === phone) {
              await redis.hdel(REDIS_KEYS.SESSIONS, sessId)
            }
          }

          pushLog(`Auto-kick | User +${phone} removed from database`)
        }
      } catch (e) {
        console.error('Auto-kick error for', phone, ':', e.message)
      }
    }
  } catch (e) {
    console.error('Auto-kick check error:', e.message)
  }
}

// Run auto-kick check every 5 minutes
setInterval(checkAndKickExpiredUsers, 5 * 60 * 1000)

// Also run once on startup (after 30 seconds to let WA connect)
setTimeout(checkAndKickExpiredUsers, 30000)
// ====== END AUTO-KICK ======

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

  // Use file-based auth (standard Baileys)
  const { state, saveCreds } = await useMultiFileAuthState('./auth')
  const { version } = await fetchLatestBaileysVersion()

  pushLog('WA | Using file-based auth')

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
        pushLog('WA | LOGGED OUT - Please scan QR again')
        // Delete auth folder
        const fs = await import('fs')
        if (fs.existsSync('./auth')) {
          fs.rmSync('./auth', { recursive: true, force: true })
          pushLog('WA | Auth folder deleted')
        }
        setTimeout(() => start(), 3000)
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

  // DISABLED: WhatsApp commands - website only mode
  /*
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
        
        if (/\bmulai\b|\bstart\b|\bsubscribe\b|\/langganan/.test(text)) {
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

        if (/\bberhenti\b|\bunsubscribe\b|\bstop\b|^\/berhenti$/.test(text)) {
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
  */
}

start().catch(e => {
  console.error('FATAL |', e.message)
  process.exit(1)
})
