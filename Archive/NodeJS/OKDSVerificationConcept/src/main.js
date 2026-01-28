const { Client, GatewayIntentBits, Partials, EmbedBuilder, PermissionFlagsBits, REST, Routes, SlashCommandBuilder } = require('discord.js')
const https = require('https')
const fs = require('fs')
const crypto = require('crypto')

const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'))

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMessages
  ],
  partials: [Partials.Channel, Partials.Message]
})

// Configuration
const VERIFICATION_TIMEOUT = 10 * 60 * 1000 // 10 minutes
const VERIFICATION_COOLDOWN = 60 * 1000 // 1 minute
const ACCOUNT_AGE_THRESHOLD = {
  DISCORD: 365 * 24 * 60 * 60 * 1000, // 1 year
  KOGAMA: 365 * 24 * 60 * 60 * 1000 // 1 year
}
const SOFT_PUNISHMENT_DURATION = 2 * 60 * 60 * 1000 // 2 hours

// State management
const pending = new Map()
const verifiedUsers = new Set()
const rateLimits = new Map()
const reviewQueue = new Map()
const softPunishmentCooldown = new Map()
const memberJoinTimes = new Map()

const generateCode = () => crypto.randomBytes(4).toString('hex').toUpperCase()
const generateReviewID = () => 'VR-' + crypto.randomBytes(3).toString('hex').toUpperCase()

const isValidKoGaMaURL = url => {
  const pattern = /^https?:\/\/(www\.|friends\.)?kogama\.(com|com\.br)\/profile\/\d+\/?$/i
  return pattern.test(url)
}

const fetchPage = (url, retries = 3) =>
  new Promise((resolve, reject) => {
    const attempt = n => {
      const req = https.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5'
        },
        timeout: 10000
      }, res => {
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode}`))
        }
        
        let data = ''
        res.on('data', chunk => data += chunk)
        res.on('end', () => resolve(data))
      })

      req.on('timeout', () => {
        req.destroy()
        if (n > 0) {
          console.log(`[FETCH] Timeout, retrying... (${retries - n + 1}/${retries})`)
          setTimeout(() => attempt(n - 1), 1000)
        } else {
          reject(new Error('Request timeout'))
        }
      })

      req.on('error', err => {
        if (n > 0) {
          console.log(`[FETCH] Error, retrying... (${retries - n + 1}/${retries})`)
          setTimeout(() => attempt(n - 1), 1000)
        } else {
          reject(err)
        }
      })
    }
    
    attempt(retries)
  })

const extractBootstrap = html => {
  try {
    const match = html.match(/options\.bootstrap\s*=\s*({[\s\S]*?});/)
    if (!match) return null
    
    const bootstrapStr = match[1]
    return Function(`"use strict"; return (${bootstrapStr})`)()
  } catch (err) {
    console.error('[PARSE] Bootstrap extraction failed:', err.message)
    return null
  }
}

const formatTimestamp = date => {
  try {
    const d = new Date(date)
    return d.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'UTC',
      timeZoneName: 'short'
    })
  } catch {
    return 'Invalid Date'
  }
}

const getAccountAgeDays = dateString => {
  try {
    const created = new Date(dateString)
    const now = new Date()
    const diffMs = now - created
    return Math.floor(diffMs / (24 * 60 * 60 * 1000))
  } catch {
    return 0
  }
}

const getMembershipDuration = userId => {
  const joinTime = memberJoinTimes.get(userId)
  if (!joinTime) return null
  return Date.now() - joinTime
}

const isExemptFromVerification = (member, userId) => {
  // User is verified if they DON'T have the unverified role
  if (!member.roles.cache.has(config.unverifiedRoleId)) return true
  
  return false
}

const isSuspiciousAccount = (discordCreated, kogamaCreated) => {
  const now = Date.now()
  const discordAge = now - new Date(discordCreated).getTime()
  const kogamaAge = now - new Date(kogamaCreated).getTime()
  
  return discordAge < ACCOUNT_AGE_THRESHOLD.DISCORD || kogamaAge < ACCOUNT_AGE_THRESHOLD.KOGAMA
}

const checkRateLimit = userId => {
  const now = Date.now()
  const lastAttempt = rateLimits.get(userId)
  
  if (lastAttempt && now - lastAttempt < VERIFICATION_COOLDOWN) {
    const remaining = Math.ceil((VERIFICATION_COOLDOWN - (now - lastAttempt)) / 1000)
    return { limited: true, remaining }
  }
  
  rateLimits.set(userId, now)
  return { limited: false }
}

const checkSoftPunishmentCooldown = userId => {
  const now = Date.now()
  const cooldownEnd = softPunishmentCooldown.get(userId)
  
  if (cooldownEnd && now < cooldownEnd) {
    const remainingMs = cooldownEnd - now
    const remainingHours = Math.floor(remainingMs / (60 * 60 * 1000))
    const remainingMinutes = Math.floor((remainingMs % (60 * 60 * 1000)) / (60 * 1000))
    return { 
      inCooldown: true, 
      remaining: `${remainingHours}h ${remainingMinutes}m`,
      remainingMs 
    }
  }
  
  return { inCooldown: false }
}

const applySoftPunishment = (userId, reason) => {
  const cooldownEnd = Date.now() + SOFT_PUNISHMENT_DURATION
  softPunishmentCooldown.set(userId, cooldownEnd)
  console.log(`[PUNISHMENT] Soft punishment applied to ${userId} for: ${reason}. Cooldown until ${new Date(cooldownEnd).toISOString()}`)
}

const handleExpiry = async (userId, data) => {
  console.log(`[EXPIRE] Verification expired for user ${userId}`)
  
  try {
    const user = await client.users.fetch(userId).catch(() => null)
    
    if (user) {
      const expiredEmbed = new EmbedBuilder()
        .setTitle('Verification Session Expired')
        .setDescription(
          `Your verification session has timed out after 10 minutes of inactivity.\n\n` +
          `If you're still in the server, use the \`/verifyme\` or \`.verifyme\` command in any channel to start a fresh verification session.\n\n` +
          `If you've been removed, simply rejoin and the verification process will begin automatically.`
        )
        .setColor(0xffa500)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()
      
      await user.send({ embeds: [expiredEmbed] }).catch(() => {
        console.log(`[EXPIRE] Could not DM user ${userId} about expiration`)
      })
    }
    
    const logChannel = client.channels.cache.get(config.logChannelId)
    if (logChannel) {
      const expiryTime = data.timestamp + VERIFICATION_TIMEOUT
      
      const logEmbed = new EmbedBuilder()
        .setTitle('⏱️ Verification Session Timeout')
        .setDescription(`A user failed to complete verification within the 10-minute window.`)
        .addFields(
          { name: '👤 User', value: user ? `${user.tag}\n\`${userId}\`` : `Unknown User\n\`${userId}\``, inline: true },
          { name: '🔑 Code Issued', value: `\`${data.code}\``, inline: true },
          { name: '⏰ Session Duration', value: '10 minutes', inline: true },
          { name: '🕐 Session Started', value: formatTimestamp(data.timestamp), inline: true },
          { name: '🕐 Expired At', value: formatTimestamp(expiryTime), inline: true },
          { name: '📊 Total Attempts', value: data.attempts > 0 ? `${data.attempts}` : 'None', inline: true }
        )
        .setColor(0x95a5a6)
        .setFooter({ text: 'Verification Log • Timeout' })
        .setTimestamp(expiryTime)
      
      await logChannel.send({ embeds: [logEmbed] }).catch(() => {})
    }
  } catch (err) {
    console.error(`[EXPIRE] Error handling expiry:`, err.message)
  }
  
  pending.delete(userId)
}

const scheduleExpiry = (userId, data) => {
  const timeUntilExpiry = VERIFICATION_TIMEOUT - (Date.now() - data.timestamp)
  
  if (timeUntilExpiry <= 0) {
    handleExpiry(userId, data)
    return
  }
  
  setTimeout(() => {
    const currentData = pending.get(userId)
    if (currentData && currentData.timestamp === data.timestamp) {
      handleExpiry(userId, data)
    }
  }, timeUntilExpiry)
}

const startVerification = async member => {
  if (!member) {
    console.log(`[VERIFY] Invalid member object`)
    return { success: false, reason: 'invalid_member' }
  }

  if (isExemptFromVerification(member, member.id)) {
    console.log(`[VERIFY] User ${member.user.tag} is exempt from verification`)
    verifiedUsers.add(member.id)
    return { success: false, reason: 'already_verified' }
  }

  const softCooldown = checkSoftPunishmentCooldown(member.id)
  if (softCooldown.inCooldown) {
    console.log(`[VERIFY] User ${member.user.tag} is in soft punishment cooldown (${softCooldown.remaining} remaining)`)
    
    const cooldownEmbed = new EmbedBuilder()
      .setTitle('Verification Cooldown Active')
      .setDescription(
        `You are currently unable to verify due to a previous verification issue.\n\n` +
        `**Time Remaining:** ${softCooldown.remaining}\n\n` +
        `This cooldown will expire automatically. Please be patient and try again once the timer runs out.`
      )
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    try {
      await member.send({ embeds: [cooldownEmbed] })
    } catch (err) {
      console.error(`[VERIFY] Failed to DM ${member.user.tag} about cooldown:`, err.message)
    }
    
    return { success: false, reason: 'soft_punishment_cooldown', remaining: softCooldown.remaining }
  }

  const rateLimit = checkRateLimit(member.id)
  if (rateLimit.limited) {
    console.log(`[VERIFY] Rate limited ${member.user.tag} (${rateLimit.remaining}s remaining)`)
    return { success: false, reason: 'rate_limited', remaining: rateLimit.remaining }
  }

  console.log(`[VERIFY] Starting verification for ${member.user.tag} (${member.id})`)

  const code = generateCode()
  const verificationData = {
    code,
    timestamp: Date.now(),
    attempts: 0
  }
  pending.set(member.id, verificationData)
  
  scheduleExpiry(member.id, verificationData)

  const embed = new EmbedBuilder()
    .setTitle('KoGaMa Account Verification')
    .setDescription(
      `To gain access to the server, please verify your KoGaMa account by following these steps:\n\n` +
      `**1. Go to your KoGaMa profile settings**\n` +
      `Open your KoGaMa account and navigate to your profile settings.\n\n` +
      `**2. Add this verification code to your account description:**\n` +
      `\`\`\`\n${code}\n\`\`\`\n` +
      `Copy the code above and paste it anywhere in your profile description.\n\n` +
      `**3. Reply to this message with your full KoGaMa profile URL**\n` +
      `Send your complete profile URL in this DM conversation.\n\n` +
      `**Accepted URL formats:**\n` +
      `• https://www.kogama.com/profile/12345678/\n` +
      `• https://kogama.com/profile/12345678/\n` +
      `• https://friends.kogama.com/profile/12345678/\n` +
      `• https://kogama.com.br/profile/12345678/\n\n` +
      `**Important:** This verification code will expire in 10 minutes.\n\n` +
      `You may remove the code from your description after verification is complete.`
    )
    .setColor(0x5865f2)
    .setFooter({ text: 'KoGaMa Verification System' })
    .setTimestamp()

  try {
    await member.send({ embeds: [embed] })
    console.log(`[VERIFY] Verification DM sent to ${member.user.tag}`)
    return { success: true }
  } catch (err) {
    console.error(`[VERIFY] Failed to DM ${member.user.tag}:`, err.message)
    pending.delete(member.id)
    
    const guild = client.guilds.cache.get(config.guildId)
    const verifyChannel = guild?.channels.cache.get(config.verifyChannelId)
    
    if (verifyChannel) {
      const dmFailEmbed = new EmbedBuilder()
        .setTitle('Verification DM Failed')
        .setDescription(
          `${member}, we couldn't send you a DM with verification instructions.\n\n` +
          `**Please enable Direct Messages from server members:**\n` +
          `1. Right-click the server icon\n` +
          `2. Go to Privacy Settings\n` +
          `3. Enable "Direct Messages"\n` +
          `4. Use \`/verifyme\` or \`.verifyme\` to try again`
        )
        .setColor(0xe74c3c)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()
      
      await verifyChannel.send({ embeds: [dmFailEmbed] }).catch(err => {
        console.error(`[VERIFY] Failed to send DM fail message to channel:`, err.message)
      })
    }
    
    return { success: false, reason: 'dm_failed' }
  }
}

const sendToManualReview = async (msg, member, profile, code, url, reason) => {
  const reviewId = generateReviewID()
  
  reviewQueue.set(reviewId, {
    userId: msg.author.id,
    username: msg.author.tag,
    memberId: member.id,
    profile,
    url,
    code,
    timestamp: Date.now(),
    reason,
    codeFoundInBio: (profile.description || '').includes(code)
  })
  
  const reviewChannel = client.channels.cache.get(config.reviewChannelId)
  
  if (!reviewChannel) {
    console.error('[REVIEW] Review channel not found')
    return
  }
  
  const discordAgeDays = getAccountAgeDays(msg.author.createdAt)
  const kogamaAgeDays = getAccountAgeDays(profile.created)
  
  const reviewEmbed = new EmbedBuilder()
    .setTitle('🔍 Manual Verification Review Required')
    .setURL(url)
    .setColor(0xf39c12)
    .setDescription(
      `**Review ID:** \`${reviewId}\`\n` +
      `**Flagged Reason:** ${reason}\n\n` +
      `**Moderator Actions:**\n` +
      `✅ Approve: \`/verify ${reviewId}\` or \`.verify ${reviewId}\`\n` +
      `❌ Deny: \`/deny ${reviewId} [reason]\` or \`.deny ${reviewId} [reason]\``
    )
    .addFields(
      { name: '👤 Discord User', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
      { name: '🎮 KoGaMa User', value: `[${profile.username}](${url})\n⭐ Level ${profile.level}`, inline: true },
      { name: '🔑 Code Match', value: reviewQueue.get(reviewId).codeFoundInBio ? '✅ Found in bio' : '❌ Not found', inline: true },
      { name: '📅 Discord Age', value: `${discordAgeDays} days\n${formatTimestamp(msg.author.createdAt)}`, inline: true },
      { name: '📅 KoGaMa Age', value: `${kogamaAgeDays} days\n${formatTimestamp(profile.created)}`, inline: true },
      { name: '🕐 Last Active', value: formatTimestamp(profile.last_ping), inline: true },
      { name: '🎫 Verification Code', value: `\`${code}\``, inline: false }
    )
    .setThumbnail(msg.author.displayAvatarURL({ dynamic: true }))
    .setFooter({ text: `Review System • ID: ${reviewId}` })
    .setTimestamp()
  
  try {
    await reviewChannel.send({
      content: `<@&${config.reviewRoleId}>`,
      embeds: [reviewEmbed]
    })
    
    console.log(`[REVIEW] Sent ${reviewId} for manual review - ${reason}`)
    
    const userEmbed = new EmbedBuilder()
      .setTitle('Verification Under Review')
      .setDescription(
        `Your verification has been flagged for manual review by our moderation team.\n\n` +
        `A moderator will review your request shortly. You will be notified once a decision has been made.\n\n` +
        `**Review ID:** \`${reviewId}\``
      )
      .setColor(0xf39c12)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    await msg.reply({ embeds: [userEmbed] })
  } catch (err) {
    console.error('[REVIEW] Failed to send review:', err.message)
  }
}

const processVerification = async (msg, url) => {
  const verificationData = pending.get(msg.author.id)
  if (!verificationData) return

  const { code, timestamp, attempts } = verificationData

  if (Date.now() - timestamp > VERIFICATION_TIMEOUT) {
    pending.delete(msg.author.id)
    
    const expiredEmbed = new EmbedBuilder()
      .setTitle('Verification Expired')
      .setDescription(
        `Your verification session has expired after 10 minutes of inactivity.\n\n` +
        `Please use \`/verifyme\` or \`.verifyme\` in the server to start a new verification session.`
      )
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    return msg.reply({ embeds: [expiredEmbed] })
  }

  verificationData.attempts = attempts + 1

  if (!isValidKoGaMaURL(url)) {
    if (attempts >= 2) {
      pending.delete(msg.author.id)
      
      const failEmbed = new EmbedBuilder()
        .setTitle('Verification Failed')
        .setDescription(
          `Invalid URL format. You've exceeded the maximum number of attempts.\n\n` +
          `Please start over with \`/verifyme\` or \`.verifyme\` in the server.`
        )
        .setColor(0xe74c3c)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()
      
      return msg.reply({ embeds: [failEmbed] })
    }
    
    const invalidEmbed = new EmbedBuilder()
      .setTitle('Invalid URL Format')
      .setDescription(
        `The URL you provided is not in the correct format.\n\n` +
        `**Accepted formats:**\n` +
        `• https://www.kogama.com/profile/12345678/\n` +
        `• https://kogama.com/profile/12345678/\n` +
        `• https://friends.kogama.com/profile/12345678/\n` +
        `• https://kogama.com.br/profile/12345678/\n\n` +
        `Please try again with a valid URL.`
      )
      .setColor(0xe67e22)
      .setFooter({ text: `Attempts remaining: ${2 - attempts} | KoGaMa Verification System` })
      .setTimestamp()
    
    return msg.reply({ embeds: [invalidEmbed] })
  }

  let html
  try {
    await msg.channel.sendTyping()
    html = await fetchPage(url)
  } catch (err) {
    console.error('[FETCH] Failed to fetch profile:', err.message)
    
    const fetchErrorEmbed = new EmbedBuilder()
      .setTitle('Profile Fetch Failed')
      .setDescription(
        `Unable to fetch your KoGaMa profile. Please verify that:\n\n` +
        `• The URL is correct\n` +
        `• Your profile is public\n` +
        `• KoGaMa servers are accessible\n\n` +
        `Please try again.`
      )
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    return msg.reply({ embeds: [fetchErrorEmbed] })
  }

  const bootstrap = extractBootstrap(html)
  if (!bootstrap?.object) {
    console.error('[PARSE] Bootstrap object not found')
    
    const parseErrorEmbed = new EmbedBuilder()
      .setTitle('Invalid Profile Page')
      .setDescription(
        `Unable to parse your KoGaMa profile page.\n\n` +
        `Please ensure the URL points to a valid, public profile page.`
      )
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    return msg.reply({ embeds: [parseErrorEmbed] })
  }

  const profile = bootstrap.object
  const description = (profile.description || '').trim()

  const guild = client.guilds.cache.get(config.guildId)
  if (!guild) {
    console.error('[GUILD] Guild not found')
    
    const configErrorEmbed = new EmbedBuilder()
      .setTitle('Server Configuration Error')
      .setDescription(`An internal error occurred. Please contact an administrator.`)
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    return msg.reply({ embeds: [configErrorEmbed] })
  }

  const member = await guild.members.fetch(msg.author.id).catch(() => null)
  if (!member) {
    console.error('[MEMBER] Member not found in guild')
    pending.delete(msg.author.id)
    
    const notInServerEmbed = new EmbedBuilder()
      .setTitle('Not In Server')
      .setDescription(`You are no longer a member of the server. Verification cancelled.`)
      .setColor(0x95a5a6)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    return msg.reply({ embeds: [notInServerEmbed] })
  }

  const codeVerified = description.includes(code)
  
  if (!codeVerified) {
    console.log(`[VERIFY] ❌ FAILED for ${msg.author.tag} - Code not found in bio`)
    
    applySoftPunishment(msg.author.id, "code_not_found_in_bio")
    
    const softPunishEmbed = new EmbedBuilder()
      .setTitle('Verification Failed')
      .setDescription(
        `The verification code was not found in your KoGaMa profile description.\n\n` +
        `**Expected code:** \`${code}\`\n` +
        `**Your profile:** ${url}\n\n` +
        `Due to security measures, you have been placed on a 2-hour cooldown.\n\n` +
        `You may attempt verification again after the cooldown period expires.`
      )
      .setColor(0xe74c3c)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    await msg.reply({ embeds: [softPunishEmbed] }).catch(err => {
      console.error('[VERIFY] Failed to send punishment DM:', err.message)
    })
    
    const logChannel = client.channels.cache.get(config.logChannelId)
    if (logChannel) {
      const logEmbed = new EmbedBuilder()
        .setTitle('❌ Verification Failed - Cooldown Applied')
        .setURL(url)
        .setColor(0xe74c3c)
        .setDescription(`User failed verification due to missing verification code in KoGaMa bio.`)
        .addFields(
          { name: '👤 Discord User', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
          { name: '🎮 KoGaMa Profile', value: `[${profile.username}](${url})\n⭐ Level ${profile.level}`, inline: true },
          { name: '🔑 Expected Code', value: `\`${code}\``, inline: true },
          { name: '✅ Code Found', value: '❌ No', inline: true },
          { name: '⚠️ Punishment', value: '⏳ 2-hour cooldown', inline: true },
          { name: '📝 Attempts Made', value: `${attempts + 1}`, inline: true },
          { name: '📄 Profile Description', value: description || '*Empty description*', inline: false },
          { name: '🕐 Failed At', value: formatTimestamp(Date.now()), inline: false }
        )
        .setThumbnail(msg.author.displayAvatarURL({ dynamic: true }))
        .setFooter({ text: 'Verification Log • Failed verification' })
        .setTimestamp()
      
      await logChannel.send({ embeds: [logEmbed] }).catch(err => {
        console.error('[VERIFY] Failed to log verification failure:', err.message)
      })
    }
    
    pending.delete(msg.author.id)
    return
  }

  const suspicious = isSuspiciousAccount(msg.author.createdAt, profile.created)
  
  if (suspicious) {
    const discordAgeDays = getAccountAgeDays(msg.author.createdAt)
    const kogamaAgeDays = getAccountAgeDays(profile.created)
    
    let reason = []
    if (discordAgeDays < 365) {
      reason.push(`Discord account is ${discordAgeDays} days old (< 1 year)`)
    }
    if (kogamaAgeDays < 365) {
      reason.push(`KoGaMa account is ${kogamaAgeDays} days old (< 1 year)`)
    }
    
    await sendToManualReview(msg, member, profile, code, url, reason.join(', '))
    pending.delete(msg.author.id)
    return
  }

  console.log(`[VERIFY] ✅ AUTO-APPROVED for ${msg.author.tag} - KoGaMa: ${profile.username}`)
  
  try {
    await member.roles.remove(config.unverifiedRoleId)
    verifiedUsers.add(msg.author.id)
    
    const successEmbed = new EmbedBuilder()
      .setTitle('Verification Successful')
      .setDescription(
        `Welcome to the server, **${profile.username}**!\n\n` +
        `You now have full access to all channels and features.\n\n` +
        `You may remove the verification code from your KoGaMa description if you wish.`
      )
      .setColor(0x2ecc71)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    await msg.reply({ embeds: [successEmbed] })
    
    const logChannel = client.channels.cache.get(config.logChannelId)
    if (logChannel) {
      const discordAgeDays = getAccountAgeDays(msg.author.createdAt)
      const kogamaAgeDays = getAccountAgeDays(profile.created)
      
      const logEmbed = new EmbedBuilder()
        .setTitle('✅ Auto-Approved Verification')
        .setURL(url)
        .setColor(0x2ecc71)
        .setDescription(`User successfully verified automatically - all criteria met.`)
        .addFields(
          { name: '👤 Discord User', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
          { name: '🎮 KoGaMa User', value: `[${profile.username}](${url})\n⭐ Level ${profile.level}\n🆔 ID: ${profile.id}`, inline: true },
          { name: '🔑 Code Match', value: '✅ Verified', inline: true },
          { name: '📅 Discord Age', value: `${discordAgeDays} days\n${formatTimestamp(msg.author.createdAt)}`, inline: true },
          { name: '📅 KoGaMa Age', value: `${kogamaAgeDays} days\n${formatTimestamp(profile.created)}`, inline: true },
          { name: '🕐 Last Active', value: formatTimestamp(profile.last_ping), inline: true },
          { name: '📝 Verification Method', value: '🤖 Automatic', inline: true },
          { name: '⏱️ Process Duration', value: `${Math.round((Date.now() - timestamp) / 1000)}s`, inline: true },
          { name: '🎫 Total Attempts', value: `${attempts + 1}`, inline: true },
          { name: '🕐 Verified At', value: formatTimestamp(Date.now()), inline: false }
        )
        .setThumbnail(msg.author.displayAvatarURL({ dynamic: true }))
        .setFooter({ text: 'Verification Log • Auto-approved' })
        .setTimestamp()
      
      await logChannel.send({ embeds: [logEmbed] })
    }
  } catch (err) {
    console.error('[VERIFY] Failed to remove unverified role:', err.message)
    
    const roleErrorEmbed = new EmbedBuilder()
      .setTitle('Role Update Failed')
      .setDescription(
        `Your verification was successful, but we couldn't update your roles.\n\n` +
        `Please contact a server administrator to manually grant you access.`
      )
      .setColor(0xe67e22)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()
    
    await msg.reply({ embeds: [roleErrorEmbed] })
  }

  pending.delete(msg.author.id)
}

// Slash command definitions
const commands = [
  new SlashCommandBuilder()
    .setName('verifyme')
    .setDescription('Start the KoGaMa verification process'),
  
  new SlashCommandBuilder()
    .setName('debug')
    .setDescription('Force start verification for a user (Admin only)')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageRoles)
    .addUserOption(option =>
      option.setName('user')
        .setDescription('User to verify (leave empty for yourself)')
        .setRequired(false)
    ),
  
  new SlashCommandBuilder()
    .setName('verify')
    .setDescription('Approve a verification review request (Admin only)')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageRoles)
    .addStringOption(option =>
      option.setName('review_id')
        .setDescription('The review ID (e.g., VR-XXXXXX)')
        .setRequired(true)
    ),
  
  new SlashCommandBuilder()
    .setName('deny')
    .setDescription('Deny a verification review request (Admin only)')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageRoles)
    .addStringOption(option =>
      option.setName('review_id')
        .setDescription('The review ID (e.g., VR-XXXXXX)')
        .setRequired(true)
    )
    .addStringOption(option =>
      option.setName('reason')
        .setDescription('Reason for denial')
        .setRequired(false)
    )
]

// Register slash commands
const rest = new REST({ version: '10' }).setToken(config.token)

;(async () => {
  try {
    console.log('[COMMANDS] Registering slash commands...')
    
    await rest.put(
      Routes.applicationGuildCommands(config.clientId, config.guildId),
      { body: commands }
    )
    
    console.log('[COMMANDS] ✅ Successfully registered slash commands')
  } catch (error) {
    console.error('[COMMANDS] Failed to register slash commands:', error)
  }
})()

// Event handlers
client.once('ready', () => {
  console.log(`[READY] ✅ Logged in as ${client.user.tag}`)
  console.log(`[READY] 🟢 Bot is active and monitoring ${client.guilds.cache.size} server(s)`)
  console.log(`[READY] 📊 Cached ${client.users.cache.size} users`)
  console.log(`[READY] 🎯 Focused on guild ID: ${config.guildId}`)
  console.log(`[READY] 🔄 Using UNVERIFIED role system (removing role on verification)`)
  
  // Bot status/presence
  client.user.setPresence({
    activities: [{ name: 'KoGaMa verifications', type: 3 }],
    status: 'online'
  })
  
  const guild = client.guilds.cache.get(config.guildId)
  if (guild) {
    guild.members.fetch().then(members => {
      members.forEach(member => {
        if (!memberJoinTimes.has(member.id)) {
          memberJoinTimes.set(member.id, member.joinedTimestamp || Date.now())
        }
        // Track users who DON'T have the unverified role (they're verified)
        if (!member.roles.cache.has(config.unverifiedRoleId)) {
          verifiedUsers.add(member.id)
        }
      })
      console.log(`[READY] 📝 Loaded ${memberJoinTimes.size} member join times`)
      console.log(`[READY] ✅ Loaded ${verifiedUsers.size} verified users`)
    }).catch(err => {
      console.error('[READY] Failed to fetch members:', err.message)
    })
  }
})

client.on('guildMemberAdd', async member => {
  if (member.guild.id !== config.guildId) return
  
  memberJoinTimes.set(member.id, Date.now())
  console.log(`[JOIN] 👋 ${member.user.tag} (${member.id}) joined the server`)
  
  // LINE 748: Role assignment on join
  try {
    await member.roles.add(config.unverifiedRoleId)
    console.log(`[JOIN] 🔒 Added unverified role to ${member.user.tag}`)
  } catch (err) {
    console.error(`[JOIN] Failed to add unverified role to ${member.user.tag}:`, err.message)
  }
  
  const result = await startVerification(member)
  
  if (!result.success) {
    console.log(`[JOIN] ⚠️ Verification start failed for ${member.user.tag}: ${result.reason}`)
  }
})

client.on('guildMemberRemove', member => {
  if (member.guild.id !== config.guildId) return
  
  memberJoinTimes.delete(member.id)
  verifiedUsers.delete(member.id)
  pending.delete(member.id)
  console.log(`[LEAVE] 👋 ${member.user.tag} (${member.id}) left the server`)
})

// Slash command handler
client.on('interactionCreate', async interaction => {
  if (!interaction.isChatInputCommand()) return
  
  const { commandName } = interaction
  
  // L/verifyme command
  if (commandName === 'verifyme') {
    const result = await startVerification(interaction.member)
    
    if (result.success) {
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('Verification started! Check your DMs for instructions.')
          .setColor(0x2ecc71)],
        ephemeral: true
      })
    } else if (result.reason === 'already_verified') {
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('You are already verified! You have full access to the server.')
          .setColor(0x2ecc71)],
        ephemeral: true
      })
    } else if (result.reason === 'rate_limited') {
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(`Please wait ${result.remaining} seconds before trying again.`)
          .setColor(0xe67e22)],
        ephemeral: true
      })
    } else if (result.reason === 'dm_failed') {
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(`Unable to send you a DM. Please check the verification channel for instructions.`)
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    } else if (result.reason === 'soft_punishment_cooldown') {
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(`Verification cooldown active. Time remaining: ${result.remaining}`)
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    return
  }
  
  // debug command
  if (commandName === 'debug') {
    const targetUser = interaction.options.getUser('user')
    const targetMember = targetUser ? await interaction.guild.members.fetch(targetUser.id).catch(() => null) : interaction.member
    
    if (!targetMember) {
      return interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('User not found in the server.')
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    
    // Clear all verification data
    pending.delete(targetMember.id)
    rateLimits.delete(targetMember.id)
    verifiedUsers.delete(targetMember.id)
    softPunishmentCooldown.delete(targetMember.id)
    
    // Add unverified role if they don't have it
    try {
      if (!targetMember.roles.cache.has(config.unverifiedRoleId)) {
        await targetMember.roles.add(config.unverifiedRoleId)
      }
    } catch (err) {
      console.error(`[DEBUG] Failed to add unverified role:`, err.message)
    }
    
    // Force start verification
    const code = generateCode()
    const verificationData = {
      code,
      timestamp: Date.now(),
      attempts: 0
    }
    pending.set(targetMember.id, verificationData)
    
    scheduleExpiry(targetMember.id, verificationData)

    const embed = new EmbedBuilder()
      .setTitle('KoGaMa Account Verification')
      .setDescription(
        `To gain access to the server, please verify your KoGaMa account by following these steps:\n\n` +
        `**1. Go to your KoGaMa profile settings**\n` +
        `Open your KoGaMa account and navigate to your profile settings.\n\n` +
        `**2. Add this verification code to your account description:**\n` +
        `\`\`\`\n${code}\n\`\`\`\n` +
        `Copy the code above and paste it anywhere in your profile description.\n\n` +
        `**3. Reply to this message with your full KoGaMa profile URL**\n` +
        `Send your complete profile URL in this DM conversation.\n\n` +
        `**Accepted URL formats:**\n` +
        `• https://www.kogama.com/profile/12345678/\n` +
        `• https://kogama.com/profile/12345678/\n` +
        `• https://friends.kogama.com/profile/12345678/\n` +
        `• https://kogama.com.br/profile/12345678/\n\n` +
        `**Important:** This verification code will expire in 10 minutes.\n\n` +
        `You may remove the code from your description after verification is complete.`
      )
      .setColor(0x5865f2)
      .setFooter({ text: 'KoGaMa Verification System' })
      .setTimestamp()

    try {
      await targetMember.send({ embeds: [embed] })
      
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(
            `**Debug Mode Activated**\n\n` +
            `**Target:** ${targetMember.user.tag}\n` +
            `**Code:** \`${code}\`\n` +
            `**Status:** Verification DM sent\n\n` +
            `All cooldowns and restrictions cleared.`
          )
          .setColor(0x3498db)],
        ephemeral: true
      })
      
      console.log(`[DEBUG] Forced verification start for ${targetMember.user.tag} with code ${code}`)
      
      const logChannel = client.channels.cache.get(config.logChannelId)
      if (logChannel) {
        const logEmbed = new EmbedBuilder()
          .setTitle('🛠️ Debug Verification Triggered')
          .setDescription(`A moderator manually triggered verification for testing/debugging purposes.`)
          .addFields(
            { name: '👤 Target User', value: `${targetMember.user.tag}\n\`${targetMember.id}\``, inline: true },
            { name: '👮 Triggered By', value: `${interaction.user.tag}\n\`${interaction.user.id}\``, inline: true },
            { name: '🔑 Code', value: `\`${code}\``, inline: true },
            { name: '⚡ Actions Taken', value: '• Cleared existing session\n• Cleared rate limits\n• Cleared cooldowns\n• Added unverified role\n• Started new session', inline: false },
            { name: '🕐 Triggered At', value: formatTimestamp(Date.now()), inline: false }
          )
          .setColor(0x3498db)
          .setFooter({ text: 'Debug Log • Manual verification trigger' })
          .setTimestamp()
        
        await logChannel.send({ embeds: [logEmbed] })
      }
    } catch (err) {
      console.error(`[DEBUG] Failed to send DM:`, err.message)
      
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(`Failed to send DM to ${targetMember.user.tag}. Check if their DMs are open.`)
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    
    return
  }
  
  // /verify command
  if (commandName === 'verify') {
    const reviewId = interaction.options.getString('review_id')
    
    const review = reviewQueue.get(reviewId)
    if (!review) {
      return interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('Review request not found. It may have already been processed or expired.')
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    
    const member = await interaction.guild.members.fetch(review.userId).catch(() => null)
    if (!member) {
      reviewQueue.delete(reviewId)
      return interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('User is no longer in the server.')
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    
    try {
      await member.roles.remove(config.unverifiedRoleId)
      verifiedUsers.add(review.userId)
      
      const successEmbed = new EmbedBuilder()
        .setTitle('Verification Approved')
        .setDescription(
          `Welcome to the server, **${review.profile.username}**!\n\n` +
          `Your verification has been manually approved by a moderator.\n\n` +
          `You now have full access to all channels and features.`
        )
        .setColor(0x2ecc71)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()
      
      await member.send({ embeds: [successEmbed] }).catch(() => {})
      
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription(`Successfully verified **${review.username}** (Review ID: \`${reviewId}\`)`)
          .setColor(0x2ecc71)],
        ephemeral: true
      })
      
      const logChannel = client.channels.cache.get(config.logChannelId)
      if (logChannel) {
        const logEmbed = new EmbedBuilder()
          .setTitle('✅ Manually Approved Verification')
          .setURL(review.url)
          .setColor(0x2ecc71)
          .setDescription(`Moderator manually approved a flagged verification request.`)
          .addFields(
            { name: '👤 Discord User', value: `${review.username}\n\`${review.userId}\``, inline: true },
            { name: '🎮 KoGaMa User', value: `[${review.profile.username}](${review.url})\n⭐ Level ${review.profile.level}`, inline: true },
            { name: '👮 Approved By', value: `${interaction.user.tag}\n\`${interaction.user.id}\``, inline: true },
            { name: '🎫 Review ID', value: `\`${reviewId}\``, inline: true },
            { name: '⚠️ Original Flag', value: review.reason, inline: false },
            { name: '🔑 Code Found', value: review.codeFoundInBio ? '✅ Yes' : '❌ No', inline: true },
            { name: '📝 Verification Method', value: '👮 Manual approval', inline: true },
            { name: '🕐 Approved At', value: formatTimestamp(Date.now()), inline: false }
          )
          .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
          .setFooter({ text: 'Verification Log • Manual approval' })
          .setTimestamp()
        
        await logChannel.send({ embeds: [logEmbed] })
      }
      
      reviewQueue.delete(reviewId)
    } catch (err) {
      console.error('[VERIFY] Manual approval failed:', err.message)
      
      await interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('Failed to verify user. Please check bot permissions and try again.')
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    return
  }
  
  // /deny command
  if (commandName === 'deny') {
    const reviewId = interaction.options.getString('review_id')
    const reason = interaction.options.getString('reason') || 'No reason provided'
    
    const review = reviewQueue.get(reviewId)
    if (!review) {
      return interaction.reply({
        embeds: [new EmbedBuilder()
          .setDescription('Review request not found. It may have already been processed or expired.')
          .setColor(0xe74c3c)],
        ephemeral: true
      })
    }
    
    const member = await interaction.guild.members.fetch(review.userId).catch(() => null)
    
    if (member) {
      const denyEmbed = new EmbedBuilder()
        .setTitle('Verification Denied')
        .setDescription(
          `Your verification request has been reviewed and denied by our moderation team.\n\n` +
          `**Reason:**\n${reason}\n\n` +
          `If you believe this was a mistake, please contact server staff for assistance.`
        )
        .setColor(0xe74c3c)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()
      
      await member.send({ embeds: [denyEmbed] }).catch(() => {})
    }
    
    await interaction.reply({
      embeds: [new EmbedBuilder()
        .setDescription(`Denied verification for **${review.username}** (Review ID: \`${reviewId}\`)`)
        .setColor(0xe74c3c)],
      ephemeral: true
    })
    
    const logChannel = client.channels.cache.get(config.logChannelId)
    if (logChannel) {
      const logEmbed = new EmbedBuilder()
        .setTitle('❌ Verification Denied by Moderator')
        .setURL(review.url)
        .setColor(0xe74c3c)
        .setDescription(`Moderator denied a flagged verification request.`)
        .addFields(
          { name: '👤 Discord User', value: `${review.username}\n\`${review.userId}\``, inline: true },
          { name: '🎮 KoGaMa User', value: `[${review.profile.username}](${review.url})\n⭐ Level ${review.profile.level}`, inline: true },
          { name: '👮 Denied By', value: `${interaction.user.tag}\n\`${interaction.user.id}\``, inline: true },
          { name: '🎫 Review ID', value: `\`${reviewId}\``, inline: true },
          { name: '⚠️ Original Flag', value: review.reason, inline: true },
          { name: '📝 Denial Reason', value: reason, inline: false },
          { name: '🔑 Code Found', value: review.codeFoundInBio ? '✅ Yes' : '❌ No', inline: true },
          { name: '🕐 Denied At', value: formatTimestamp(Date.now()), inline: false }
        )
        .setFooter({ text: 'Verification Log • Denied' })
        .setTimestamp()
      
      await logChannel.send({ embeds: [logEmbed] })
    }
    
    reviewQueue.delete(reviewId)
    return
  }
})

// Message-based command handler (for backward compatibility)
client.on('messageCreate', async msg => {
  if (msg.author.bot) return

  if (msg.guild && msg.guild.id === config.guildId) {
    const content = msg.content.trim()
    const args = content.split(/\s+/)
    const command = args[0].toLowerCase()

    // .verifyme command
    if (command === '.verifyme') {
      const result = await startVerification(msg.member)
      
      if (result.success) {
        const startedEmbed = new EmbedBuilder()
          .setDescription('Verification started! Check your DMs for instructions.')
          .setColor(0x2ecc71)
        
        await msg.reply({ embeds: [startedEmbed] })
      } else if (result.reason === 'already_verified') {
        const alreadyEmbed = new EmbedBuilder()
          .setDescription('You are already verified! You have full access to the server.')
          .setColor(0x2ecc71)
        
        await msg.reply({ embeds: [alreadyEmbed] })
      } else if (result.reason === 'rate_limited') {
        const rateLimitEmbed = new EmbedBuilder()
          .setDescription(`Please wait ${result.remaining} seconds before trying again.`)
          .setColor(0xe67e22)
        
        await msg.reply({ embeds: [rateLimitEmbed] })
      } else if (result.reason === 'dm_failed') {
        const dmFailEmbed = new EmbedBuilder()
          .setDescription(`Unable to send you a DM. Please check the verification channel for instructions.`)
          .setColor(0xe74c3c)
        
        await msg.reply({ embeds: [dmFailEmbed] })
      } else if (result.reason === 'soft_punishment_cooldown') {
        const cooldownEmbed = new EmbedBuilder()
          .setDescription(`Verification cooldown active. Time remaining: ${result.remaining}`)
          .setColor(0xe74c3c)
        
        await msg.reply({ embeds: [cooldownEmbed] })
      }
      return
    }

    // .debug command
    if (command === '.debug' && msg.member.permissions.has(PermissionFlagsBits.ManageRoles)) {
      const targetUser = msg.mentions.members.first() || msg.member
      
      pending.delete(targetUser.id)
      rateLimits.delete(targetUser.id)
      verifiedUsers.delete(targetUser.id)
      softPunishmentCooldown.delete(targetUser.id)
      
      try {
        if (!targetUser.roles.cache.has(config.unverifiedRoleId)) {
          await targetUser.roles.add(config.unverifiedRoleId)
        }
      } catch (err) {
        console.error(`[DEBUG] Failed to add unverified role:`, err.message)
      }
      
      const code = generateCode()
      const verificationData = {
        code,
        timestamp: Date.now(),
        attempts: 0
      }
      pending.set(targetUser.id, verificationData)
      
      scheduleExpiry(targetUser.id, verificationData)

      const embed = new EmbedBuilder()
        .setTitle('KoGaMa Account Verification')
        .setDescription(
          `To gain access to the server, please verify your KoGaMa account by following these steps:\n\n` +
          `**1. Go to your KoGaMa profile settings**\n` +
          `Open your KoGaMa account and navigate to your profile settings.\n\n` +
          `**2. Add this verification code to your account description:**\n` +
          `\`\`\`\n${code}\n\`\`\`\n` +
          `Copy the code above and paste it anywhere in your profile description.\n\n` +
          `**3. Reply to this message with your full KoGaMa profile URL**\n` +
          `Send your complete profile URL in this DM conversation.\n\n` +
          `**Accepted URL formats:**\n` +
          `• https://www.kogama.com/profile/12345678/\n` +
          `• https://kogama.com/profile/12345678/\n` +
          `• https://friends.kogama.com/profile/12345678/\n` +
          `• https://kogama.com.br/profile/12345678/\n\n` +
          `**Important:** This verification code will expire in 10 minutes.\n\n` +
          `You may remove the code from your description after verification is complete.`
        )
        .setColor(0x5865f2)
        .setFooter({ text: 'KoGaMa Verification System' })
        .setTimestamp()

      try {
        await targetUser.send({ embeds: [embed] })
        
        const debugEmbed = new EmbedBuilder()
          .setDescription(
            `**Debug Mode Activated**\n\n` +
            `**Target:** ${targetUser.user.tag}\n` +
            `**Code:** \`${code}\`\n` +
            `**Status:** Verification DM sent\n\n` +
            `All cooldowns and restrictions cleared.`
          )
          .setColor(0x3498db)
        
        await msg.reply({ embeds: [debugEmbed] })
        
        console.log(`[DEBUG] Forced verification start for ${targetUser.user.tag} with code ${code}`)
        
        const logChannel = client.channels.cache.get(config.logChannelId)
        if (logChannel) {
          const logEmbed = new EmbedBuilder()
            .setTitle('🛠️ Debug Verification Triggered')
            .setDescription(`A moderator manually triggered verification for testing/debugging purposes.`)
            .addFields(
              { name: '👤 Target User', value: `${targetUser.user.tag}\n\`${targetUser.id}\``, inline: true },
              { name: '👮 Triggered By', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
              { name: '🔑 Code', value: `\`${code}\``, inline: true },
              { name: '⚡ Actions Taken', value: '• Cleared existing session\n• Cleared rate limits\n• Cleared cooldowns\n• Added unverified role\n• Started new session', inline: false },
              { name: '🕐 Triggered At', value: formatTimestamp(Date.now()), inline: false }
            )
            .setColor(0x3498db)
            .setFooter({ text: 'Debug Log • Manual verification trigger' })
            .setTimestamp()
          
          await logChannel.send({ embeds: [logEmbed] })
        }
      } catch (err) {
        console.error(`[DEBUG] Failed to send DM:`, err.message)
        
        const errorEmbed = new EmbedBuilder()
          .setDescription(`Failed to send DM to ${targetUser.user.tag}. Check if their DMs are open.`)
          .setColor(0xe74c3c)
        
        await msg.reply({ embeds: [errorEmbed] })
      }
      
      return
    }

    // .verify command
    if (command === '.verify' && msg.member.permissions.has(PermissionFlagsBits.ManageRoles)) {
      const reviewId = args[1]
      
      if (!reviewId) {
        return msg.reply('Please provide a review ID. Usage: `.verify VR-XXXXXX`')
      }
      
      const review = reviewQueue.get(reviewId)
      if (!review) {
        return msg.reply('Review request not found. It may have already been processed or expired.')
      }
      
      const member = await msg.guild.members.fetch(review.userId).catch(() => null)
      if (!member) {
        reviewQueue.delete(reviewId)
        return msg.reply('User is no longer in the server.')
      }
      
      try {
        await member.roles.remove(config.unverifiedRoleId)
        verifiedUsers.add(review.userId)
        
        const successEmbed = new EmbedBuilder()
          .setTitle('Verification Approved')
          .setDescription(
            `Welcome to the server, **${review.profile.username}**!\n\n` +
            `Your verification has been manually approved by a moderator.\n\n` +
            `You now have full access to all channels and features.`
          )
          .setColor(0x2ecc71)
          .setFooter({ text: 'KoGaMa Verification System' })
          .setTimestamp()
        
        await member.send({ embeds: [successEmbed] }).catch(() => {})
        
        const modEmbed = new EmbedBuilder()
          .setDescription(`Successfully verified **${review.username}** (Review ID: \`${reviewId}\`)`)
          .setColor(0x2ecc71)
        
        await msg.reply({ embeds: [modEmbed] })
        
        const logChannel = client.channels.cache.get(config.logChannelId)
        if (logChannel) {
          const logEmbed = new EmbedBuilder()
            .setTitle('✅ Manually Approved Verification')
            .setURL(review.url)
            .setColor(0x2ecc71)
            .setDescription(`Moderator manually approved a flagged verification request.`)
            .addFields(
              { name: '👤 Discord User', value: `${review.username}\n\`${review.userId}\``, inline: true },
              { name: '🎮 KoGaMa User', value: `[${review.profile.username}](${review.url})\n⭐ Level ${review.profile.level}`, inline: true },
              { name: '👮 Approved By', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
              { name: '🎫 Review ID', value: `\`${reviewId}\``, inline: true },
              { name: '⚠️ Original Flag', value: review.reason, inline: false },
              { name: '🔑 Code Found', value: review.codeFoundInBio ? '✅ Yes' : '❌ No', inline: true },
              { name: '📝 Verification Method', value: '👮 Manual approval', inline: true },
              { name: '🕐 Approved At', value: formatTimestamp(Date.now()), inline: false }
            )
            .setThumbnail(member.user.displayAvatarURL({ dynamic: true }))
            .setFooter({ text: 'Verification Log • Manual approval' })
            .setTimestamp()
          
          await logChannel.send({ embeds: [logEmbed] })
        }
        
        reviewQueue.delete(reviewId)
      } catch (err) {
        console.error('[VERIFY] Manual approval failed:', err.message)
        
        const errorEmbed = new EmbedBuilder()
          .setDescription('Failed to verify user. Please check bot permissions and try again.')
          .setColor(0xe74c3c)
        
        await msg.reply({ embeds: [errorEmbed] })
      }
      return
    }

    // .deny command
    if (command === '.deny' && msg.member.permissions.has(PermissionFlagsBits.ManageRoles)) {
      const reviewId = args[1]
      const reason = args.slice(2).join(' ') || 'No reason provided'
      
      if (!reviewId) {
        return msg.reply('Please provide a review ID. Usage: `.deny VR-XXXXXX [reason]`')
      }
      
      const review = reviewQueue.get(reviewId)
      if (!review) {
        return msg.reply('Review request not found. It may have already been processed or expired.')
      }
      
      const member = await msg.guild.members.fetch(review.userId).catch(() => null)
      
      if (member) {
        const denyEmbed = new EmbedBuilder()
          .setTitle('Verification Denied')
          .setDescription(
            `Your verification request has been reviewed and denied by our moderation team.\n\n` +
            `**Reason:**\n${reason}\n\n` +
            `If you believe this was a mistake, please contact server staff for assistance.`
          )
          .setColor(0xe74c3c)
          .setFooter({ text: 'KoGaMa Verification System' })
          .setTimestamp()
        
        await member.send({ embeds: [denyEmbed] }).catch(() => {})
      }
      
      const modEmbed = new EmbedBuilder()
        .setDescription(`Denied verification for **${review.username}** (Review ID: \`${reviewId}\`)`)
        .setColor(0xe74c3c)
      
      await msg.reply({ embeds: [modEmbed] })
      
      const logChannel = client.channels.cache.get(config.logChannelId)
      if (logChannel) {
        const logEmbed = new EmbedBuilder()
          .setTitle('❌ Verification Denied by Moderator')
          .setURL(review.url)
          .setColor(0xe74c3c)
          .setDescription(`Moderator denied a flagged verification request.`)
          .addFields(
            { name: '👤 Discord User', value: `${review.username}\n\`${review.userId}\``, inline: true },
            { name: '🎮 KoGaMa User', value: `[${review.profile.username}](${review.url})\n⭐ Level ${review.profile.level}`, inline: true },
            { name: '👮 Denied By', value: `${msg.author.tag}\n\`${msg.author.id}\``, inline: true },
            { name: '🎫 Review ID', value: `\`${reviewId}\``, inline: true },
            { name: '⚠️ Original Flag', value: review.reason, inline: true },
            { name: '📝 Denial Reason', value: reason, inline: false },
            { name: '🔑 Code Found', value: review.codeFoundInBio ? '✅ Yes' : '❌ No', inline: true },
            { name: '🕐 Denied At', value: formatTimestamp(Date.now()), inline: false }
          )
          .setFooter({ text: 'Verification Log • Denied' })
          .setTimestamp()
        
        await logChannel.send({ embeds: [logEmbed] })
      }
      
      reviewQueue.delete(reviewId)
      return
    }
  }

  if (!msg.guild && pending.has(msg.author.id)) {
    let url = msg.content.trim()
    url = url.replace(/^<(.+)>$/, '$1')
    await processVerification(msg, url)
  }
})

client.on('error', err => {
  console.error('[CLIENT] Error:', err)
})

process.on('unhandledRejection', err => {
  console.error('[PROCESS] Unhandled rejection:', err)
})

process.on('SIGINT', () => {
  console.log('[PROCESS] Shutting down gracefully...')
  client.destroy()
  process.exit(0)
})

client.login(config.token).catch(err => {
  console.error('[LOGIN] Failed to login:', err)
  process.exit(1)
})
