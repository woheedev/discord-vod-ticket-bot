import { Client, GatewayIntentBits } from "discord.js";
import * as dotenv from "dotenv";
import chalk from "chalk";

dotenv.config();

const requiredEnvVars = ["TOKEN", "DRY_RUN"];
requiredEnvVars.forEach((varName) => {
  if (!process.env[varName]) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
});

const DRY_RUN = process.env.DRY_RUN === "true";
const MAIN_SERVER_ID = "1309266911703334952";
const REVIEW_CHANNELS = {
  tank: {
    channelId: "1316181886308978788",
    leadRoleId: "1311763115733418136",
    classRoleIds: [
      "1315087293408739401",
      "1315087506105958420",
      "1315087805650571366",
    ],
  },
  healer: {
    channelId: "1316182043012632626",
    leadRoleId: "1311763190828371978",
    classRoleIds: [
      "1315090429233991812",
      "1315090436703912058",
      "1315090738500993115",
      "1315091030248263690",
    ],
  },
  ranged: {
    channelId: "1316182011362414693",
    leadRoleId: "1311763275293130783",
    classRoleIds: [
      "1315091763370786898",
      "1315091966303797248",
      "1315092313755881573",
    ],
  },
  melee: {
    channelId: "1316181992177668237",
    leadRoleId: "1315188182404300861",
    classRoleIds: ["1315092445930717194", "1315093022483939338"],
  },
  bomber: {
    channelId: "1316182023433486427",
    leadRoleId: "1315188211097534495",
    classRoleIds: ["1315092575509807215", "1315092852690128907"],
  },
};

const Logger = {
  formatMessage: (type, msg) => `[${new Date().toISOString()}] ${type} ${msg}`,
  info: (msg) => console.log(chalk.blue(Logger.formatMessage("INFO", msg))),
  thread: (msg) =>
    console.log(chalk.green(Logger.formatMessage("THREAD", msg))),
  warn: (msg) => console.log(chalk.yellow(Logger.formatMessage("WARN", msg))),
  error: (msg) => console.log(chalk.red(Logger.formatMessage("ERROR", msg))),
};

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

async function retryOperation(operation, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      Logger.info(`Starting operation attempt ${i + 1}/${maxRetries}`);
      const result = await Promise.race([
        operation(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Operation timed out")), 5000)
        ),
      ]);
      Logger.info("Operation completed successfully");
      return result;
    } catch (error) {
      Logger.error(`Operation failed with error: ${error.message}`);
      if (i === maxRetries - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
      Logger.warn(`Retrying operation, attempt ${i + 2}/${maxRetries}`);
    }
  }
}

const migrateThreadName = async (thread, dryRun = true) => {
  const match = thread.name.match(/^(.+) - (.+) Review$/);
  if (!match) return null;

  const [, displayName, classRole] = match;
  const guildMembers = await thread.guild.members.fetch();
  const user = guildMembers.find(
    (member) =>
      member.nickname === displayName || member.user.username === displayName
  );

  const newThreadName = user
    ? formatThreadName(displayName, classRole, user.id)
    : formatThreadName(displayName, classRole, "MISSING");

  if (dryRun) {
    Logger.info(`[DRY RUN] Would rename thread:
        From: ${thread.name}
        To: ${newThreadName}
        UserID: ${user ? user.id : "MISSING"}
        Archived: ${thread.archived}
        Locked: ${thread.locked}`);
    return {
      oldName: thread.name,
      newName: newThreadName,
      userId: user ? user.id : "MISSING",
      userFound: !!user,
    };
  }

  try {
    Logger.info(`Starting thread rename operation for: ${thread.name}`);

    // Store initial state
    const wasArchived = thread.archived;
    const wasLocked = thread.locked;

    // Unarchive and unlock if needed
    if (wasArchived) {
      Logger.info(`Unarchiving thread: ${thread.name}`);
      await retryOperation(() => thread.setArchived(false));
    }
    if (wasLocked) {
      Logger.info(`Unlocking thread: ${thread.name}`);
      await retryOperation(() => thread.setLocked(false));
    }

    // Rename thread
    Logger.info(`Attempting to set name to: ${newThreadName}`);
    await retryOperation(async () => {
      Logger.info("Executing setName operation...");
      await thread.setName(newThreadName);
      Logger.info("setName operation completed");
    });

    // Restore original state
    if (wasLocked) {
      Logger.info(`Restoring locked state for: ${newThreadName}`);
      await retryOperation(() => thread.setLocked(true));
    }
    if (wasArchived) {
      Logger.info(`Restoring archived state for: ${newThreadName}`);
      await retryOperation(() => thread.setArchived(true));
    }

    Logger.thread(`Successfully migrated thread: ${newThreadName}`);

    if (thread.autoArchiveDuration !== 10080) {
      Logger.info("Updating archive duration...");
      await retryOperation(() => thread.setAutoArchiveDuration(10080));
      Logger.thread(`Updated autoArchiveDuration for thread: ${newThreadName}`);
    }

    return {
      oldName: thread.name,
      newName: newThreadName,
      userId: user ? user.id : "MISSING",
      userFound: !!user,
      wasArchived,
      wasLocked,
    };
  } catch (error) {
    Logger.error(`Error migrating thread ${thread.name}: ${error.stack}`);
    return null;
  }
};

const formatThreadName = (displayName, classRole, userId) => {
  return `${displayName} - ${classRole} Review [${userId}]`;
};

const cleanupBotMessages = async (mainGuild) => {
  Logger.info("Starting bot message cleanup...");
  const BOT_ID = "1315474283853643806";
  const MESSAGE_PREFIX =
    "Thread name changes are restricted to bot and administrators only.";

  for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
    const channel = await mainGuild.channels.fetch(channelData.channelId, {
      force: true,
    });
    if (!channel) continue;

    Logger.info(`Checking channel: ${channel.name} for bot messages`);

    const [activeThreads, archivedThreads] = await Promise.all([
      channel.threads.fetch({ active: true, force: true }),
      channel.threads.fetch({ archived: true, fetchAll: true, force: true }),
    ]);

    const allThreads = [
      ...activeThreads.threads.values(),
      ...archivedThreads.threads.values(),
    ];

    for (const thread of allThreads) {
      try {
        const messages = await thread.messages.fetch();
        const botMessages = messages.filter(
          (msg) =>
            msg.author.id === BOT_ID && msg.content.startsWith(MESSAGE_PREFIX)
        );

        for (const message of botMessages.values()) {
          Logger.info(`Deleting bot message in thread: ${thread.name}`);
          await message.delete();
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Rate limit protection
        }
      } catch (error) {
        Logger.error(
          `Error cleaning up messages in thread ${thread.name}: ${error}`
        );
      }
    }
  }
  Logger.info("Bot message cleanup completed");
};

client.on("ready", async () => {
  Logger.info(`Migration Bot logged in as ${client.user.tag}`);
  if (DRY_RUN) Logger.info("Running in DRY RUN mode");

  try {
    const mainGuild = await client.guilds.fetch(MAIN_SERVER_ID);
    await cleanupBotMessages(mainGuild);
    Logger.info("Starting thread name migration...");
    const migrationResults = [];

    // Clear client cache
    client.channels.cache.clear();
    mainGuild.channels.cache.clear();

    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      // Force fetch channel instead of using cache
      const channel = await mainGuild.channels.fetch(channelData.channelId, {
        force: true,
      });
      if (!channel) continue;

      Logger.info(`Checking channel: ${channel.name}`);

      // Force fetch threads with cache busting
      const [activeThreads, archivedThreads] = await Promise.all([
        channel.threads.fetch({ active: true, force: true }),
        channel.threads.fetch({ archived: true, fetchAll: true, force: true }),
      ]);

      const allThreads = [
        ...activeThreads.threads.values(),
        ...archivedThreads.threads.values(),
      ];

      Logger.info(
        `Found ${allThreads.length} total threads in ${channel.name}`
      );

      for (const thread of allThreads) {
        if (!thread.name.includes("[")) {
          try {
            const result = await migrateThreadName(thread, DRY_RUN);
            if (result) {
              migrationResults.push({ ...result, channelId: channel.id });
            }
          } catch (error) {
            Logger.error(`Failed to migrate thread ${thread.name}: ${error}`);
          }
          // Add a delay between each thread rename to avoid rate limits
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
      }
    }

    Logger.info("\nMigration Summary:");
    Logger.info(`Total threads checked: ${migrationResults.length}`);
    Logger.info(
      `Detailed results: ${JSON.stringify(migrationResults, null, 2)}`
    );
  } catch (error) {
    Logger.error(`Error during migration: ${error}`);
  }

  client.destroy();
});

client.login(process.env.TOKEN);
