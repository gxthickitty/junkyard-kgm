
Add the following to your `config.json`:
```json
{
  "token": "YOUR_BOT_TOKEN",
  "clientId": "YOUR_BOT_CLIENT_ID",
  "guildId": "YOUR_GUILD_ID",
  "unverifiedRoleId": "UNVERIFIED_ROLE_ID",
  "logChannelId": "LOG_CHANNEL_ID",
  "verifyChannelId": "VERIFY_CHANNEL_ID",
  "reviewChannelId": "REVIEW_CHANNEL_ID",
  "reviewRoleId": "REVIEW_ROLE_ID"
}
```

## Command Locations

### User Commands

#### `/verifyme` and `.verifyme` Command
- **Slash Command Definition:** Line 632-634
- **Slash Command Handler:** Line 780-816
- **Text Command Handler:** Line 1090-1118
- **Description:** Starts the KoGaMa verification process for the user
- **Usage:** 
  - Slash: `/verifyme`
  - Text: `.verifyme`

---

### Admin Commands (Require ManageRoles Permission)

#### `/debug` and `.debug` Command
- **Slash Command Definition:** Line 636-642
- **Slash Command Handler:** Line 817-918
- **Text Command Handler:** Line 1119-1201
- **Description:** Force starts verification for a user, clearing all cooldowns and restrictions
- **Usage:**
  - Slash: `/debug` or `/debug user:@User`
  - Text: `.debug` or `.debug @User`

#### `/verify` and `.verify` Command
- **Slash Command Definition:** Line 644-651
- **Slash Command Handler:** Line 920-1011
- **Text Command Handler:** Line 1202-1279
- **Description:** Manually approve a verification review request
- **Usage:**
  - Slash: `/verify review_id:VR-XXXXXX`
  - Text: `.verify VR-XXXXXX`

#### `/deny` and `.deny` Command
- **Slash Command Definition:** Line 653-664
- **Slash Command Handler:** Line 1013-1088
- **Text Command Handler:** Line 1280-1348
- **Description:** Deny a verification review request with optional reason
- **Usage:**
  - Slash: `/deny review_id:VR-XXXXXX reason:Reason here`
  - Text: `.deny VR-XXXXXX Reason here`

---

## Core Features

### Bot Status/Presence
- **Location:** Line 717-721
- **Description:** Sets the bot's activity to "Watching KoGaMa verifications"
- **Current Value:** `'KoGaMa verifications'` with type `3` (Watching)

### Role Assignment on Member Join
- **Location:** Line 748-753
- **Description:** Automatically adds the unverified role when a new member joins
- **Function:** `guildMemberAdd` event handler

### Slash Command Registration
- **Location:** Line 667-680
- **Description:** Registers all slash commands with Discord API
- **Note:** Requires `clientId` in config.json

### Command Definitions Array
- **Location:** Line 632-664
- **Description:** Array of SlashCommandBuilder objects for all commands

---

## Important Functions

### `startVerification(member)`
- **Location:** Line 263-386
- **Description:** Initiates the verification process for a member
- **Returns:** Object with `success` boolean and optional `reason` string

### `processVerification(msg, url)`
- **Location:** Line 467-678
- **Description:** Processes KoGaMa profile URL and verifies the user
- **Handles:** URL validation, profile fetching, code verification, account age checks

### `sendToManualReview(msg, member, profile, code, url, reason)`
- **Location:** Line 388-465
- **Description:** Sends flagged verifications to review channel for manual approval

---

## Event Handlers

### `ready` Event
- **Location:** Line 683-716
- **Description:** Fires when bot successfully connects to Discord
- **Actions:** Sets presence, loads member data, caches verified users

### `guildMemberAdd` Event
- **Location:** Line 733-756
- **Description:** Fires when a new member joins the server
- **Actions:** Adds unverified role, starts verification process

### `guildMemberRemove` Event
- **Location:** Line 758-765
- **Description:** Fires when a member leaves the server
- **Actions:** Cleans up user data from Maps

### `interactionCreate` Event
- **Location:** Line 767-1088
- **Description:** Handles all slash command interactions

### `messageCreate` Event
- **Location:** Line 1090-1355
- **Description:** Handles text-based commands and DM verification responses

---

## Configuration Constants

### Timeouts and Cooldowns
- **Location:** Line 18-25
- `VERIFICATION_TIMEOUT`: 10 minutes (600000ms)
- `VERIFICATION_COOLDOWN`: 1 minute (60000ms)
- `ACCOUNT_AGE_THRESHOLD`: 1 year for both Discord and KoGaMa
- `SOFT_PUNISHMENT_DURATION`: 2 hours (7200000ms)

### State Management Maps
- **Location:** Line 27-33
- `pending`: Active verification sessions
- `verifiedUsers`: Set of verified user IDs
- `rateLimits`: Rate limit tracking
- `reviewQueue`: Manual review requests
- `softPunishmentCooldown`: Punishment cooldowns
- `memberJoinTimes`: Member join timestamps

---

## Dependencies Required

```json
{
  "discord.js": "^14.x.x"
}
```

### Required Discord.js Imports
- **Location:** Line 1
- `Client`, `GatewayIntentBits`, `Partials`, `EmbedBuilder`, `PermissionFlagsBits`, `REST`, `Routes`, `SlashCommandBuilder`

---

## Bot Permissions Required

### Discord Permissions
- `GUILDS` - Access to guild information
- `GUILD_MEMBERS` - Access to member information
- `DIRECT_MESSAGES` - Send DMs to users
- `MESSAGE_CONTENT` - Read message content (for text commands)
- `GUILD_MESSAGES` - Send messages in guild channels
- `MANAGE_ROLES` - Add/remove roles during verification

### Bot Intents
- **Location:** Line 8-15
- Guilds, GuildMembers, DirectMessages, MessageContent, GuildMessages

---

## Quick Start Guide

1. **Install dependencies:**
   ```bash
   npm install discord.js
   ```

2. **Create config.json with required values**

3. **Run the bot:**
   ```bash
   node bot-updated.js
   ```

4. **Slash commands will be automatically registered** upon startup

5. **Both `/` and `.` commands will work simultaneously**

---

## Notes

- Both slash commands and text commands work identically
- Slash commands provide better UX with auto-complete and validation
- Text commands remain for backward compatibility
- Admin commands require `ManageRoles` permission
- All commands are guild-specific (not global)
- DM verification responses work the same regardless of command type used

---

## Troubleshooting

### Slash Commands Not Appearing
1. Ensure `clientId` is in config.json
2. Check bot has `applications.commands` scope
3. Wait up to 1 hour for Discord to cache commands
4. Check console for registration errors

### Commands Not Responding
1. Verify bot has required permissions
2. Check intents are properly configured
3. Ensure bot is in the correct guild
4. Review console logs for errors
