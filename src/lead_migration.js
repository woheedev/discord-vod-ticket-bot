import { Client, GatewayIntentBits } from "discord.js";
import * as dotenv from "dotenv";
import chalk from "chalk";

dotenv.config();

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

// Old class lead role IDs that we want to check
const CLASS_LEAD_ROLES = {
  TANK: "1311763115733418136",
  HEALER: "1311763190828371978",
  RANGED: "1311763275293130783",
  MELEE: "1315188182404300861",
  BOMBER: "1315188211097534495",
};

// Users that should be removed from all threads (unless they're the thread owner)
const USERS_TO_REMOVE = ["637828900965318662", "113756786649071616"];

const MAIN_SERVER_ID = "1309266911703334952";

// Review channels with their weapon lead roles
const REVIEW_CHANNELS = {
  tank: {
    channelId: "1316181886308978788",
    leadRoleId: CLASS_LEAD_ROLES.TANK,
    classRoleIds: [
      "1315087293408739401", // SNS / GS
      "1315087506105958420", // SNS / Wand
      "1315087805650571366", // SNS / Dagger
      "1323213957195894805", // SNS / Spear
    ],
    weaponLeadRoleIds: {
      "1315087293408739401": "1323121646336479253", // SNS / GS Lead
      "1315087506105958420": "1323121710861516901", // SNS / Wand Lead
      "1315087805650571366": "1323121684147994756", // SNS / Dagger Lead
      "1323213957195894805": "1324201709886509107", // SNS / Spear Lead
    },
  },
  healer: {
    channelId: "1316182043012632626",
    leadRoleId: CLASS_LEAD_ROLES.HEALER,
    classRoleIds: [
      "1315090429233991812", // Wand / Bow
      "1315090436703912058", // Wand / Staff
      "1315090738500993115", // Wand / SNS
      "1315091030248263690", // Wand / Dagger
    ],
    weaponLeadRoleIds: {
      "1315090429233991812": "1323122250995597442", // Wand / Bow Lead
      "1315090436703912058": "1323122341995348078", // Wand / Staff Lead
      "1315090738500993115": "1323122486396715101", // Wand / SNS Lead
      "1315091030248263690": "1323122572174299160", // Wand / Dagger Lead
    },
  },
  ranged: {
    channelId: "1316182011362414693",
    leadRoleId: CLASS_LEAD_ROLES.RANGED,
    classRoleIds: [
      "1315091763370786898", // Staff / Bow
      "1315091966303797248", // Staff / Dagger
      "1315092313755881573", // Bow / Dagger
    ],
    weaponLeadRoleIds: {
      "1315091763370786898": "1323122828802920479", // Staff / Bow Lead
      "1315091966303797248": "1323122917466181672", // Staff / Dagger Lead
      "1315092313755881573": "1323122947040219166", // Bow / Dagger Lead
    },
  },
  melee: {
    channelId: "1316181992177668237",
    leadRoleId: CLASS_LEAD_ROLES.MELEE,
    classRoleIds: [
      "1315092445930717194", // GS / Dagger
      "1323213919002689559", // Spear / Dagger
      "1315093022483939338", // Spear / Other
    ],
    weaponLeadRoleIds: {
      "1315092445930717194": "1323123053793640560", // GS / Dagger Lead
      "1323213919002689559": "1323123139500048384", // Spear / Dagger Lead
      "1315093022483939338": "1324201778190880799", // Spear / Other Lead
    },
  },
  bomber: {
    channelId: "1316182023433486427",
    leadRoleId: CLASS_LEAD_ROLES.BOMBER,
    classRoleIds: [
      "1315092575509807215", // Dagger / Wand
      "1315092852690128907", // Xbow / Dagger
    ],
    weaponLeadRoleIds: {
      "1315092575509807215": "1323123176405729393", // Dagger / Wand Lead
      "1315092852690128907": "1323123243959451671", // Xbow / Dagger Lead
    },
  },
};

const Logger = {
  formatMessage: (type, msg) => `[${new Date().toISOString()}] ${type} ${msg}`,
  info: (msg) => console.log(chalk.blue(Logger.formatMessage("INFO", msg))),
  warn: (msg) => console.log(chalk.yellow(Logger.formatMessage("WARN", msg))),
  error: (msg) => console.log(chalk.red(Logger.formatMessage("ERROR", msg))),
  success: (msg) =>
    console.log(chalk.green(Logger.formatMessage("SUCCESS", msg))),
};

async function migrateThreadLeads() {
  try {
    const guild = await client.guilds.fetch(MAIN_SERVER_ID);
    Logger.info(`Connected to guild: ${guild.name}`);

    // Process each review channel
    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      const channel = await guild.channels.fetch(channelData.channelId);
      if (!channel) {
        Logger.warn(`Could not find channel for ${className}`);
        continue;
      }

      Logger.info(`Processing ${className} channel...`);

      // Get all threads in the channel
      const [activeThreads, archivedThreads] = await Promise.all([
        channel.threads.fetchActive(),
        channel.threads.fetchArchived(),
      ]);

      const allThreads = [
        ...activeThreads.threads.values(),
        ...archivedThreads.threads.values(),
      ];
      Logger.info(`Found ${allThreads.length} threads in ${className}`);

      // Process each thread
      for (const thread of allThreads) {
        try {
          Logger.info(`Processing thread: ${thread.name}`);

          // Get thread owner's weapon role
          const userIdMatch = thread.name.match(/\[(\d+)\]$/);
          if (!userIdMatch) {
            Logger.warn(
              `Could not find user ID in thread name: ${thread.name}`
            );
            continue;
          }

          const userId = userIdMatch[1];
          const member = await guild.members.fetch(userId).catch(() => null);
          if (!member) {
            Logger.warn(
              `Could not find member ${userId} for thread ${thread.name}`
            );
            continue;
          }

          // Find their weapon role
          const weaponRole = member.roles.cache.find((role) =>
            channelData.classRoleIds.includes(role.id)
          );
          if (!weaponRole) {
            Logger.warn(
              `Could not find weapon role for ${member.user.tag} in thread ${thread.name}`
            );
            continue;
          }

          // Get the corresponding weapon lead role ID
          const weaponLeadRoleId = channelData.weaponLeadRoleIds[weaponRole.id];
          if (!weaponLeadRoleId) {
            Logger.warn(
              `No weapon lead role found for weapon ${weaponRole.name}`
            );
            continue;
          }

          // Get current thread members
          const threadMembers = await thread.members.fetch();

          // Process each thread member
          for (const [memberId, threadMember] of threadMembers) {
            // Skip the thread owner
            if (memberId === userId) continue;

            try {
              const guildMember = await guild.members.fetch(memberId);

              // Check if user is in the USERS_TO_REMOVE array
              if (USERS_TO_REMOVE.includes(memberId)) {
                await thread.members.remove(memberId);
                Logger.success(
                  `Removed user ${guildMember.user.tag} from thread ${thread.name} (in USERS_TO_REMOVE list)`
                );
                continue;
              }

              // Only check members who have the class lead role
              if (guildMember.roles.cache.has(channelData.leadRoleId)) {
                // If they don't have the specific weapon lead role, remove them
                if (!guildMember.roles.cache.has(weaponLeadRoleId)) {
                  await thread.members.remove(memberId);
                  Logger.success(
                    `Removed class lead ${guildMember.user.tag} from thread ${thread.name} (no matching weapon lead role)`
                  );
                } else {
                  Logger.info(
                    `Kept class lead ${guildMember.user.tag} in thread ${thread.name} (has matching weapon lead role)`
                  );
                }
              }
            } catch (error) {
              Logger.error(
                `Error processing member ${memberId} in thread ${thread.name}: ${error.message}`
              );
            }
          }
        } catch (error) {
          Logger.error(
            `Error processing thread ${thread.name}: ${error.message}`
          );
        }
      }
    }

    Logger.success("Migration completed successfully!");
    process.exit(0);
  } catch (error) {
    Logger.error(`Migration failed: ${error.message}`);
    process.exit(1);
  }
}

client.once("ready", () => {
  Logger.info(`Bot logged in as ${client.user.tag}`);
  migrateThreadLeads();
});

client.login(process.env.TOKEN);
