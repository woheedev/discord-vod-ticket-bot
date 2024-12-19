import {
  Client,
  GatewayIntentBits,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  AuditLogEvent,
} from "discord.js";
import * as dotenv from "dotenv";
import AsyncLock from "async-lock";
import chalk from "chalk";

dotenv.config();
const lock = new AsyncLock();
const REVIEW_LOCK_KEY = "review_operations";

const requiredEnvVars = ["TOKEN", "DRY_RUN"];
requiredEnvVars.forEach((varName) => {
  if (!process.env[varName]) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
});

const DRY_RUN = process.env.DRY_RUN === "true";
const MAIN_SERVER_ID = "1309266911703334952";
const OPEN_REVIEW_CHANNEL = "1316198871462051900";
const NOTIFICATIONS_CHANNEL = "1309287447863099486";
const ADMIN_USER_ID = "107391298171891712";

const reviewThreads = new Map();

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
  sync: (msg) => console.log(chalk.cyan(Logger.formatMessage("SYNC", msg))),
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

const validateReviewThreads = async () => {
  for (const [userId, review] of reviewThreads.entries()) {
    try {
      const channel = await client.channels.fetch(review.channelId);
      const thread = await channel.threads.fetch(review.threadId);

      if (!thread || thread.archived) {
        Logger.warn(`Cleaning up stale review for user ${userId}`);
        reviewThreads.delete(userId);
      }
    } catch (error) {
      Logger.error(`Error validating review for user ${userId}: ${error}`);
      reviewThreads.delete(userId);
    }
  }
};

async function retryOperation(operation, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000 * (i + 1)));
      Logger.warn(`Retrying operation, attempt ${i + 2}/${maxRetries}`);
    }
  }
}

const isReviewChannel = (channelId) => {
  return Object.values(REVIEW_CHANNELS).some(
    (channel) => channel.channelId === channelId
  );
};

const formatThreadName = (displayName, classRole, userId) => {
  return `${displayName} - ${classRole} Review [${userId}]`;
};

const createIngameNameModal = (prefill) => {
  const modal = new ModalBuilder()
    .setCustomId("ingame_name_modal")
    .setTitle("What is your in-game name?");

  const ingameNameInput = new TextInputBuilder()
    .setCustomId("ingame_name")
    .setLabel("In-Game Name:")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setMinLength(1)
    .setMaxLength(32)
    .setValue(prefill);

  modal.addComponents(new ActionRowBuilder().addComponents(ingameNameInput));
  return modal;
};

const handleReviewCreation = async (
  interaction,
  matchingClass,
  channel,
  ingameName
) => {
  return await lock.acquire(REVIEW_LOCK_KEY, async () => {
    const existingReview = reviewThreads.get(interaction.user.id);
    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(`close_review_${interaction.user.id}`)
        .setLabel("Close Review")
        .setStyle(ButtonStyle.Danger)
    );

    if (existingReview) {
      const reviewChannel = interaction.guild.channels.cache.get(
        existingReview.channelId
      );
      if (reviewChannel) {
        const thread = await reviewChannel.threads.fetch(
          existingReview.threadId
        );
        if (thread && existingReview.archived) {
          await thread.setArchived(false);
          await thread.setLocked(false);
          await thread.send({
            content: `This thread has been reopened by <@${interaction.user.id}>.`,
            allowedMentions: { users: [] },
            components: [row],
          });
          Logger.thread(
            `Reopened review thread for ${interaction.user.tag}: ${thread.name}`
          );

          // Update the reopened thread in reviewThreads
          reviewThreads.set(interaction.user.id, {
            threadId: thread.id,
            channelId: reviewChannel.id,
            className: matchingClass[0],
            leadRoleId: matchingClass[1].leadRoleId,
            archived: false,
          });

          Logger.info(
            `Added to reviewThreads: ${interaction.user.id} -> ${JSON.stringify(
              reviewThreads.get(interaction.user.id)
            )}`
          );

          return thread;
        }
      }
    }

    const userClassRole = matchingClass[1].classRoleIds
      .map((roleId) => interaction.guild.roles.cache.get(roleId))
      .find((role) => interaction.member.roles.cache.has(role.id));

    const classLeadRole = interaction.guild.roles.cache.get(
      matchingClass[1].leadRoleId
    );

    const threadTitle = formatThreadName(
      ingameName,
      userClassRole.name,
      interaction.user.id
    );

    try {
      const thread = await retryOperation(() =>
        channel.threads.create({
          name: threadTitle,
          autoArchiveDuration: 10080,
          type: 12,
        })
      );

      const promises = [
        retryOperation(() => thread.members.add(interaction.user.id)),
        retryOperation(() =>
          thread.send({
            content: `New review thread created by <@${interaction.user.id}>.`,
            allowedMentions: { users: [] },
            components: [row],
          })
        ),
      ];

      if (classLeadRole) {
        // First fetch all members with the lead role
        const classLeadMembers = await interaction.guild.members.fetch();
        const leadMembers = classLeadMembers.filter((member) =>
          member.roles.cache.has(classLeadRole.id)
        );

        promises.push(
          ...leadMembers.map((member) =>
            retryOperation(() => thread.members.add(member.id))
          )
        );
      }

      await Promise.all(promises);

      reviewThreads.set(interaction.user.id, {
        threadId: thread.id,
        channelId: channel.id,
        className: matchingClass[0],
        leadRoleId: matchingClass[1].leadRoleId,
        archived: false,
      });

      await channel.send({
        content: `New review thread created by ${interaction.member.displayName}: <#${thread.id}>`,
        allowedMentions: { users: [] },
      });
      Logger.info(`Posted message in the class channel: ${channel.name}`);

      Logger.info(
        `Added to reviewThreads: ${interaction.user.id} -> ${JSON.stringify(
          reviewThreads.get(interaction.user.id)
        )}`
      );

      Logger.thread(
        `Created review thread for ${interaction.user.tag}: ${threadTitle}`
      );
      return thread;
    } catch (error) {
      reviewThreads.delete(interaction.user.id);
      Logger.error(`Error creating review thread: ${error}`);
      throw error;
    }
  });
};

const handleReviewClosure = async (thread, interaction) => {
  try {
    await interaction.deferReply({ ephemeral: true });

    await thread.send({
      content: `This thread was closed by <@${interaction.user.id}>.`,
      allowedMentions: { users: [] },
    });
    await thread.setLocked(true);
    await thread.setArchived(true);

    await interaction.editReply({
      content: "✅ Review thread closed and archived!",
    });

    // Update the thread status in reviewThreads
    const review = reviewThreads.get(interaction.user.id);
    if (review) {
      review.archived = true;
      reviewThreads.set(interaction.user.id, review);
    }
  } catch (error) {
    Logger.error(`Error closing review thread: ${error}`);
    await interaction.editReply({
      content: "There was an error closing the review thread.",
    });
  }
};

client.on("ready", async () => {
  Logger.info(`Bot logged in as ${client.user.tag}`);
  if (DRY_RUN) Logger.info("Running in DRY RUN mode");

  try {
    const mainGuild = await client.guilds.fetch(MAIN_SERVER_ID);

    const reviewChannel = mainGuild.channels.cache.get(OPEN_REVIEW_CHANNEL);

    if (reviewChannel) {
      const messages = await reviewChannel.messages.fetch({ limit: 100 });
      const existingMessage = messages.find(
        (msg) =>
          msg.author.id === client.user.id &&
          msg.content.includes(
            "Click the button below to open a review thread:"
          )
      );

      if (!existingMessage) {
        const row = new ActionRowBuilder().addComponents(
          new ButtonBuilder()
            .setCustomId("open_review")
            .setLabel("Open Review")
            .setStyle(ButtonStyle.Primary)
        );

        await reviewChannel.send({
          content: "Click the button below to open a review thread:",
          components: [row],
        });
      }
    }

    for (const [className, channelData] of Object.entries(REVIEW_CHANNELS)) {
      const channel = mainGuild.channels.cache.get(channelData.channelId);
      if (!channel) continue;

      const [activeThreads, archivedThreads] = await Promise.all([
        channel.threads.fetchActive(),
        channel.threads.fetchArchived({
          fetchAll: true,
          type: "private",
        }),
      ]);

      const allThreads = [
        ...activeThreads.threads.values(),
        ...archivedThreads.threads.values(),
      ];

      const userThreads = new Map();

      allThreads.forEach(async (thread) => {
        const userIdMatch = thread.name.match(/\[(\d+)\]$/);
        if (!userIdMatch) return;

        const userId = userIdMatch[1];
        const existingThread = userThreads.get(userId);

        if (existingThread) {
          try {
            const member = await mainGuild.members.fetch(userId);
            const displayName = member.nickname || member.user.username;
            const notificationChannel = mainGuild.channels.cache.get(
              NOTIFICATIONS_CHANNEL
            );

            Logger.warn(
              `User ${userId} (${displayName}) has multiple threads:\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`
            );

            if (notificationChannel) {
              await notificationChannel.send({
                content: `⚠️ Multiple threads detected for ${displayName} (${userId}):\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`,
              });
            }
          } catch (error) {
            Logger.warn(
              `User ${userId} (user left server) has multiple threads:\nExisting: <#${existingThread.threadId}>\nNew: <#${thread.id}>`
            );
          }
          if (!existingThread.archived && thread.archived) {
            return;
          }
        }

        userThreads.set(userId, {
          threadId: thread.id,
          channelId: channel.id,
          className: className,
          leadRoleId: channelData.leadRoleId,
          archived: thread.archived,
        });
      });

      userThreads.forEach((value, key) => {
        reviewThreads.set(key, value);
      });
    }

    Logger.info("All reviews populated from existing threads");

    const notificationChannel = mainGuild.channels.cache.get(
      NOTIFICATIONS_CHANNEL
    );
    if (notificationChannel) {
      const activeReviewsList = await Promise.all(
        Array.from(reviewThreads.entries()).map(async ([userId, review]) => {
          const user = await mainGuild.members.fetch(userId);
          return `User: ${user.user.tag}, Thread: <#${review.threadId}>`;
        })
      );

      if (activeReviewsList.length === 0) {
        await notificationChannel.send({ content: "No active reviews found." });
      } else {
        // Split into chunks of ~1900 chars to stay well under Discord's 2000 limit
        const chunks = ["Active Reviews Detected:"];
        let currentChunk = 0;

        for (const review of activeReviewsList) {
          if (chunks[currentChunk].length + review.length + 1 > 1900) {
            currentChunk++;
            chunks[currentChunk] = "";
          }
          chunks[currentChunk] += "\n" + review;
        }

        // Send each chunk as separate message
        for (const chunk of chunks) {
          await notificationChannel.send({ content: chunk });
          Logger.info(`Sent reviews chunk of length: ${chunk.length}`);
        }
      }
    }
  } catch (error) {
    Logger.error(`Error during ready event: ${error}`);
  }

  setInterval(async () => {
    try {
      await validateReviewThreads();
    } catch (error) {
      Logger.error(`Error in review validation: ${error}`);
    }
  }, 5 * 60 * 1000);
});

client.on("interactionCreate", async (interaction) => {
  if (interaction.isButton()) {
    if (interaction.customId === "open_review") {
      const existingReview = reviewThreads.get(interaction.user.id);
      if (existingReview && !existingReview.archived) {
        await interaction.reply({
          content: `You already have an active review thread: <#${existingReview.threadId}>`,
          ephemeral: true,
        });
        return;
      }

      const userRoles = interaction.member.roles.cache;
      const matchingClass = Object.entries(REVIEW_CHANNELS).find(
        ([, channelData]) =>
          channelData.classRoleIds.some((roleId) => userRoles.has(roleId))
      );

      if (!matchingClass) {
        await interaction.reply({
          content: "You must have a valid class role to open a review thread.",
          ephemeral: true,
        });
        return;
      }

      const prefill = interaction.member.nickname || interaction.user.username;
      await interaction.showModal(createIngameNameModal(prefill));
    }

    const [action, review, userId] = interaction.customId.split("_");

    if (action === "close" && review === "review") {
      const reviewData = reviewThreads.get(userId);
      if (!reviewData || reviewData.archived) {
        await interaction.reply({
          content: "No active review found.",
          ephemeral: true,
        });
        return;
      }

      const channel = interaction.guild.channels.cache.get(
        reviewData.channelId
      );
      if (!channel) {
        await interaction.reply({
          content: "Review channel not found.",
          ephemeral: true,
        });
        return;
      }

      try {
        const thread = await channel.threads.fetch(reviewData.threadId);
        if (!thread) {
          await interaction.reply({
            content: "Review thread not found.",
            ephemeral: true,
          });
          reviewThreads.delete(userId);
          return;
        }

        await handleReviewClosure(thread, interaction);
        reviewThreads.set(userId, { ...reviewData, archived: true });
      } catch (error) {
        await interaction.reply({
          content: "There was an error closing the review thread.",
          ephemeral: true,
        });
      }
    }
  }

  if (interaction.isModalSubmit()) {
    if (interaction.customId === "ingame_name_modal") {
      const ingameName = interaction.fields.getTextInputValue("ingame_name");

      const userRoles = interaction.member.roles.cache;
      const matchingClass = Object.entries(REVIEW_CHANNELS).find(
        ([, channelData]) =>
          channelData.classRoleIds.some((roleId) => userRoles.has(roleId))
      );

      const [, channelData] = matchingClass;
      const channel = interaction.guild.channels.cache.get(
        channelData.channelId
      );

      try {
        const thread = await handleReviewCreation(
          interaction,
          matchingClass,
          channel,
          ingameName
        );
        await interaction.reply({
          content: `New review thread created: <#${thread.id}>`,
          ephemeral: true,
        });
      } catch (error) {
        await interaction.reply({
          content: "There was an error creating the review thread.",
          ephemeral: true,
        });
      }
    }
  }
});

client.on("threadUpdate", async (oldThread, newThread) => {
  if (!isReviewChannel(newThread.parentId)) return;

  const auditLogs = await newThread.guild.fetchAuditLogs({
    type: AuditLogEvent.ThreadUpdate,
    limit: 1,
  });

  const updateLog = auditLogs.entries.first();
  const executor = updateLog?.executor;

  if (executor.id !== client.user.id && executor.id !== ADMIN_USER_ID) {
    Logger.warn(
      `User ${executor.tag} (${executor.id}) attempted to rename thread ${oldThread.name} to ${newThread.name}`
    );
    try {
      await newThread.send({
        content: `Thread name changes are restricted to bot and administrators only.\nAttempted by: <@${executor.id}> (${executor.tag})\n<@107391298171891712> please revert this change.`,
        allowedMentions: { users: ["107391298171891712"] },
      });
    } catch (error) {
      Logger.error(`Error sending warning message: ${error}`);
    }
  }
});

client.on("threadDelete", async (thread) => {
  const userIdMatch = thread.name.match(/\[(\d+)\]$/);
  if (userIdMatch) {
    const userId = userIdMatch[1];
    if (reviewThreads.has(userId)) {
      reviewThreads.delete(userId);
      Logger.info(
        `Removed review thread for user ${userId} from reviewThreads map due to thread deletion.`
      );
    }
  }
});

client.login(process.env.TOKEN);
